import requests

class AnnasArchiveTorrentsRepository:

	FILE_URL = 'https://annas-archive.org/dyn/small_file/torrents/'
	URL = 'https://annas-archive.org/dyn/torrents.json'

	def list(self):
		resp = requests.get(self.URL, timeout=120)
		resp.raise_for_status()

		return resp.json()

	def get_one(self, path: str):
		resp = requests.get(self.FILE_URL + path, timeout=20)
		return resp.content

