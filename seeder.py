from dataclasses import dataclass
import os
from pathlib import Path
from posix import rename, unlink
import sys
import time
from typing import Any, Dict, List
from models.seed import SeedModel
from repositories.seeds import SeedsRepository
from utils.db import connect_db
from utils.torrent import FailedToGetMetadataException, FileNotFoundException, TorrentDownloader
import glob
import requests


@dataclass
class SeederTorrent:
	magnet_link: str
	seeds: List[SeedModel]
	torrent_handle: Any
	complete: bool = False

	ipfs_processed = False

	def load_complete_prop(self):
		for item in self.seeds:
			if not item.is_complete:
				self.complete = False
				return self

		self.complete = True

		return self


class Seeder:

	torrents: Dict[str, SeederTorrent]
	seeds: Dict[int, SeedModel]

	IPFS_GATEWAYS = [
		'http://127.0.0.1:8080',
		'https://ipfs.io',
	]

	def __init__(self):
		self.db = connect_db()
		self.cur = self.db.cursor()

		self.seeds_repo = SeedsRepository(self.db, self.cur)
		self.download_dir = "./downloads"
		self.downloader = TorrentDownloader(self.download_dir)

		self.torrents = {}
		self.seeds = {}

	def get_torrents_to_seed(self, seeds: List[SeedModel]):
		seeds_by_magneet: Dict[str, List[SeedModel]]
		seeds_by_magneet = {}

		self.seeds = {}
		for seed in seeds:
			if not seed.seed_id:
				continue

			self.seeds[seed.seed_id] = seed

			if not seed.magnet_link in seeds_by_magneet:
				seeds_by_magneet[seed.magnet_link] = []

			seeds_by_magneet[seed.magnet_link].append(seed)

		return seeds_by_magneet

	def start_torrents(self, seeds_by_magneet: Dict[str, List[SeedModel]]):
		self.torrents = {}
		for magneet in seeds_by_magneet:
			seeds = seeds_by_magneet[magneet]
			self.start_torrent(magneet, seeds)

	def start_torrent(self, magneet: str, seeds: List[SeedModel]):
		files = [seed.filename for seed in seeds]

		try:
			handle = self.downloader.add(magneet, files)
			seeder_torrent = SeederTorrent(
				magnet_link=magneet,
				seeds=seeds,
				torrent_handle=handle,
			).load_complete_prop()

			self.torrents[magneet] = seeder_torrent
			print(f"added torrent {magneet} with files {files}")
		except FailedToGetMetadataException as e:
			print(e)
		except FileNotFoundException as e:
			print(e)

	def set_complete(self, item: SeederTorrent):
		item.complete = True
		for seed in item.seeds:
			if seed.seed_id:
				path = self.downloader.get_torrent_file_path_by_name(
					item.torrent_handle, seed.filename
				)
				self.seeds_repo.set_complete(seed.seed_id, path)

		self.downloader.save_resume_data(item.torrent_handle)
		print("Torrent complete", item.magnet_link)

	def check_status(self):
		for item in self.torrents.values():
			print("Torrent status:", item.magnet_link)
			status = self.downloader.torrent_status(item.torrent_handle)

			torrent_progress = status.get('progress', 0.0)
			if not item.complete and torrent_progress == 1.0:
				self.set_complete(item)

			print(status)

	def _group_by_magnet(self, seeds: List[SeedModel]):
		groupped = {}

		for seed in seeds:
			if seed.magnet_link not in groupped:
				groupped[seed.magnet_link] = []

			groupped[seed.magnet_link].append(seed)

		return groupped

	def try_download_ipfs(self):
		for t in self.torrents.values():
			if t.complete or t.ipfs_processed:
				continue

			files_map = {}
			for seed in t.seeds:
				if not seed.ipfs_cid:
					continue

				ipfs_filename = self.download_from_ipfs(seed)
				if ipfs_filename is not None:
					files_map[seed.filename] = ipfs_filename

			if t.complete or not len(files_map.keys()):
				continue

			self.downloader.pause_torrent(t.torrent_handle)

			for filename in files_map.keys():
				ipfs_path = files_map[filename]
				path = self.downloader.get_torrent_file_path_by_name(t.torrent_handle, filename)
				if ipfs_path is None or path is None:
					continue

				dest = os.path.join(self.download_dir, path)
				Path(os.path.dirname(dest)).mkdir(exist_ok=True, parents=True)
				rename(ipfs_path, dest)

			self.downloader.resume_torrent(t.torrent_handle)
			self.downloader.force_recheck_torrent(t.torrent_handle)
			t.ipfs_processed = True

	def download_from_ipfs(self, seed: SeedModel):
		assert(seed.ipfs_cid is not None)
		ipfs_cids = seed.ipfs_cid.split(';')

		for gw in self.IPFS_GATEWAYS:
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

	def sync_seeds(self):
		print("checking for sync")
		seeds: Dict[int, SeedModel]
		seeds = { (seed.seed_id or -1): seed for seed in self.seeds_repo.list() }

		to_add = []
		to_remove = []

		for seed_id in seeds.keys():
			if seed_id not in self.seeds:
				to_add.append(seeds[seed_id])

		for seed_id in self.seeds.keys():
			if seed_id not in seeds:
				to_remove.append(self.seeds[seed_id])

		# remove seeds
		to_remove_groupped = self._group_by_magnet(to_remove)

		# removing torrent, files
		for magnet_link in to_remove_groupped.keys():
			t = self.torrents[magnet_link]

			torrent_dir = os.path.join(
				self.download_dir,
				t.torrent_handle.status().name
			)

			self.downloader.remove_resume_data(t.torrent_handle)
			self.downloader.remove_torrent(t.torrent_handle, delete_files=False)
			del self.torrents[magnet_link]

			for seed in to_remove_groupped[magnet_link]:
				paths = glob.glob(torrent_dir + '/**/' + seed.filename, recursive=True)
				if len(paths):
					print("deleting file", paths[0])
					unlink(paths[0])

		if len(to_add) or len(to_remove):
			print("restarting torrents")
			torrents = self.get_torrents_to_seed(list(seeds.values()))
			self.start_torrents(torrents)


	def main(self):
		seeds = self.seeds_repo.list()
		torrents = self.get_torrents_to_seed(seeds)
		self.start_torrents(torrents)
		try:
			while True:
				time.sleep(10)
				self.downloader.process_alerts()
				self.check_status()
				self.sync_seeds()
				self.try_download_ipfs()

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