from dataclasses import dataclass
import sys
import time
from typing import Any, Dict, List
from models.seed import SeedModel
from repositories.seeds import SeedsRepository
from utils.db import connect_db
from utils.torrent import FailedToGetMetadataExtension, TorrentDownloader


@dataclass
class SeederTorrent:
	magnet_link: str
	files: List[str]
	torrent_handle: Any


class Seeder:

	torrents: List[SeederTorrent]

	def __init__(self):
		self.db = connect_db()
		self.cur = self.db.cursor()

		self.seeds_repo = SeedsRepository(self.db, self.cur)
		self.downloader = TorrentDownloader()

		self.torrents = []

	def get_torrents_to_seed(self):
		seeds = self.seeds_repo.list()

		files_by_magneet: Dict[str, List[str]]
		files_by_magneet = {}

		for seed in seeds:
			if not seed.magnet_link in files_by_magneet:
				files_by_magneet[seed.magnet_link] = []

			files_by_magneet[seed.magnet_link].append(seed.filename)

		return files_by_magneet

	def start_torrents(self, files_by_magneet: Dict[str, List[str]]):
		for magneet in files_by_magneet:
			files = files_by_magneet[magneet]
			try:
				handle = self.downloader.add(magneet, files)
				self.torrents.append(SeederTorrent(
					magnet_link=magneet,
					files=files,
					torrent_handle=handle,
				))
				print(f"added torrent {magneet} with files {files}")
			except FailedToGetMetadataExtension as e:
				print(e)

	def print_status(self):
		for item in self.torrents:
			print("Torrent status:", item.magnet_link)
			status = self.downloader.torrent_status(item.torrent_handle)
			print(status)

	def main(self):
		torrents = self.get_torrents_to_seed()
		self.start_torrents(torrents)
		try:
			while True:
				time.sleep(10)
				self.print_status()

		except KeyboardInterrupt:
			print("Saving resume data")
			for item in self.torrents:
				self.downloader.save_resume_data(item.torrent_handle)

			sys.exit()


if __name__ == '__main__':
	Seeder().main()