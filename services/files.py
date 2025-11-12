from typing import List, Optional
from models.file import FileModel
from repositories.files import FilesRepository
from repositories.torrents import TorrentsRepository

class FilesService:
	def __init__(self, db, cursor) -> None:
		self.torrents_repo = TorrentsRepository(db, cursor)
		self.files_repo = FilesRepository(db, cursor)

	def add_file(self, file: FileModel) -> Optional[int]:
		if file.torrent:
			torrent_id = self.torrents_repo.insert(file.torrent)
			file.torrent_id = torrent_id

		file_id = self.files_repo.insert(file)
		if file_id:
			self.files_repo.insert_fts(file_id, file)
			self.files_repo.link_to_languages(file_id, file.languages)

		return file_id

	def search(self, query_text: str, language=None, year=None, limit=50, offset=0, order_by=None) -> List[FileModel]:
		return self.files_repo.search(query_text, language, year, limit, offset, order_by)
