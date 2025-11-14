from dataclasses import dataclass
import os
from pathlib import Path
from posix import rename, unlink
import sys
import time
from typing import Any, Dict, List, Optional
from models.file import FileModel
from models.torrent import TorrentFileModel, TorrentModel
from repositories.files import FilesRepository
from repositories.torrents import TorrentsRepository
from repositories.aa_torrents import AnnasArchiveTorrentsRepository

from services.torrent import TorrentService
from utils.db import connect_db
from utils.torrent import FailedToGetMetadataException, FileNotFoundException, TorrentDownloader
import glob
import requests
from config import DOWNLOADS_DIR, IPFS_GATEWAYS


@dataclass
class SeederTorrent:
	model: TorrentModel
	torrent_handle: Any
	complete: bool = False


class Seeder:

	torrents: Dict[int, SeederTorrent]

	def __init__(self):
		self.db = connect_db()
		self.cur = self.db.cursor()

		self.torrents_repo = TorrentsRepository(self.db, self.cur)
		self.aa_torrents_repo = AnnasArchiveTorrentsRepository()

		self.torrents_svc = TorrentService(self.db, self.cur)
		self.files_repo = FilesRepository(self.db, self.cur)

		self.download_dir = DOWNLOADS_DIR
		self.downloader = TorrentDownloader(self.download_dir)

		self.torrents = {}

	def sync_torrents(self):
		fresh = {t.torrent_id: t for t in self.torrents_svc.list_seeding()}

		to_remove: List[SeederTorrent]
		to_add = []
		to_remove = []

		for torrent_id in fresh:
			if torrent_id not in self.torrents:
				to_add.append(fresh[torrent_id])
				break

			fresh_files = set(map(lambda f: f.filename, fresh[torrent_id].files))
			exst_files = set(map(lambda f: f.filename, self.torrents[torrent_id].model.files))

			if exst_files != fresh_files:
				to_add.append(fresh[torrent_id])

		for torrent_id in self.torrents:
			if torrent_id not in fresh:
				to_remove.append(self.torrents[torrent_id])

		for ts in to_remove:
			self.downloader.remove_torrent(ts.torrent_handle, delete_files=False)

			assert(ts.model.torrent_id)
			del self.torrents[ts.model.torrent_id]

		self.start_torrents(to_add)

	def start_torrents(self, torrents: List[TorrentModel]):
		for model in torrents:
			self.start_torrent(model)

		for model in torrents:
			if not model.torrent_id:
				continue

			st = self.torrents.get(model.torrent_id)
			if not st:
				continue

			self.try_download_ipfs(st)

	def start_torrent(self, model: TorrentModel):
		assert(model.magnet_link)

		try:
			if model.is_seed_all:
				files = []
			else:
				files = list(map(lambda f: f.filename, model.files))
				if not len(files):
					print(f"Empty torrent {model.path}. skipping")
					return

			handle = self.downloader.add(model.magnet_link, files)
			seeder_torrent = SeederTorrent(
				model=model,
				torrent_handle=handle,
			)

			assert(model.torrent_id)
			self.torrents[model.torrent_id] = seeder_torrent
			print(f"added torrent {model.path} with files {model.files} via magnet_link")
		except FailedToGetMetadataException as e:
			print(e)
			try:
				print("Trying to add via torrent file")
				self.start_via_torrent_file(model)
			except Exception as e:
				print(e)

		except FileNotFoundException as e:
			print(e)

	def start_via_torrent_file(self, model: TorrentModel):
		assert(model.magnet_link)

		torrent_path = f"{self.download_dir}/{self.downloader.infohash_from_magnet(model.magnet_link)}.torrent"
		data = self.aa_torrents_repo.get_one(model.path)
		with open(torrent_path, 'wb') as f:
			f.write(data)

		if model.is_seed_all:
			files = []
		else:
			files = list(map(lambda f: f.filename, model.files))

		handle = self.downloader.add(torrent_path, files)
		seeder_torrent = SeederTorrent(
			model=model,
			torrent_handle=handle,
		)

		assert(model.torrent_id)
		self.torrents[model.torrent_id] = seeder_torrent
		print(f"added torrent {model.path} with files {model.files} via .torrent")

	def _set_torrent_files_complete(self, item: SeederTorrent):
		for file in item.model.files:
			if file.is_complete:
				continue

			file.is_complete = True

			# write complete to db
			local_path = self.downloader.get_torrent_file_path_by_name(
				item.torrent_handle,
				file.filename
			)

			assert(file.torrent_file_id)
			self.torrents_repo.set_file_complete(file.torrent_file_id, local_path)

	def _create_torrent_file_record(
		self,
		item: SeederTorrent,
		torrent_paths_by_basename: Dict[str, str],
		file: FileModel
	):
		assert(item.model.torrent_id)
		server_paths = file.server_path.split(';')
		tf_filename = None
		local_path = None

		for path in server_paths:
			filename = os.path.basename(path)
			if filename in torrent_paths_by_basename:
				local_path = torrent_paths_by_basename[filename]
				tf_filename = filename
				break

		if local_path is None or tf_filename is None:
			return

		assert(file.file_id)

		# create torrent_file
		tf_model = TorrentFileModel(
			torrent_file_id=-1,
			torrent_id=item.model.torrent_id,
			filename=tf_filename,
			file_id=file.file_id,
			is_complete=True,
			local_path=local_path,
		)
		self.torrents_repo.insert_file(tf_model)

	def _create_torrent_files_for_downloaded(self, item: SeederTorrent):
		assert(item.model.torrent_id)

		torrent_paths_by_basename = {
			os.path.basename(f): f
			for f in self.downloader.torrent_files(item.torrent_handle)
		}

		offset = 0
		while True:
			self.db.execute("BEGIN")

			files = self.files_repo.search(
				torrent_id=item.model.torrent_id,
				limit=100,
				offset=offset
			)

			if not len(files):
				break

			for file in files:
				try:
					self._create_torrent_file_record(item, torrent_paths_by_basename, file)
				except Exception as e:
					print(e)

			self.db.commit()
			offset += 100


	def set_complete(self, item: SeederTorrent):
		item.complete = True

		self._set_torrent_files_complete(item)

		if len(item.model.files) == 0:
			# create torrent_files records for downloaded data
			self._create_torrent_files_for_downloaded(item)

		self.downloader.save_resume_data(item.torrent_handle)
		print("Torrent complete", item.model.path)

	def check_status(self):
		for item in self.torrents.values():
			print("Torrent status:", item.model.path)
			status = self.downloader.torrent_status(item.torrent_handle)

			torrent_progress = status.get('progress', 0.0)
			if not item.complete and torrent_progress == 1.0:
				self.set_complete(item)

			del status['files']
			print(status)

	def try_download_ipfs(self, st: SeederTorrent):
		if not len(IPFS_GATEWAYS):
			return

		if st.complete or st.model.is_seed_all:
			return

		if len(st.model.files) > 10:
			return

		print("Trying IPFS for torrent", st.model.path)

		files_map = {}

		file_ids = [f.file_id for f in st.model.files]
		file_models = self.files_repo.find_by_ids(file_ids)

		for file_model in file_models:
			if not file_model.ipfs_cid:
				continue

			ipfs_filename = self.download_from_ipfs(file_model)
			if ipfs_filename is not None:
				paths = file_model.server_path.split(';')
				files_map[paths[0]] = ipfs_filename

		if st.complete or not len(files_map.keys()):
			return

		self.downloader.pause_torrent(st.torrent_handle)

		for filename in files_map.keys():
			ipfs_path = files_map[filename]
			path = self.downloader.get_torrent_file_path_by_name(st.torrent_handle, filename)
			if ipfs_path is None or path is None:
				continue

			dest = os.path.join(self.download_dir, path)
			Path(os.path.dirname(dest)).mkdir(exist_ok=True, parents=True)
			rename(ipfs_path, dest)

		self.downloader.force_recheck_torrent(st.torrent_handle)
		time.sleep(5)
		self.downloader.resume_torrent(st.torrent_handle)

		print(f"Processed {len(files_map.keys())} IPFS files for torrent", st.model.path)

	def download_from_ipfs(self, file: FileModel):
		assert(file.ipfs_cid is not None)

		ipfs_cids = file.ipfs_cid.split(';')
		ipfs_cids.sort()  # ba... cids first

		for gw in IPFS_GATEWAYS:
			for cid in ipfs_cids:
				try:
					filename = f'{self.download_dir}/.ipfs.{cid}'
					print(f"Trying to download {gw}/ipfs/{cid}")
					resp = requests.get(f"{gw}/ipfs/{cid}", timeout=10)
					resp.raise_for_status()
					with open(filename, 'wb') as f:
						for chunk in resp.iter_content(chunk_size=8192):
							f.write(chunk)

					return filename
				except Exception as e:
					print('Download failed', e)


	def main(self):

		torrents_to_seed = self.torrents_svc.list_seeding()
		self.start_torrents(torrents_to_seed)
		try:
			while True:
				time.sleep(10)
				self.downloader.process_alerts()
				self.check_status()

				try:
					self.sync_torrents()
					# self.try_download_ipfs()
				except Exception as e:
					print(e)

		except KeyboardInterrupt:
			print("Saving resume data")
			for item in self.torrents.values():
				self.downloader.save_resume_data(item.torrent_handle)

			for _ in range(5):
				self.downloader.process_alerts()
				time.sleep(0.5)

			sys.exit()


if __name__ == '__main__':
	Seeder().main()