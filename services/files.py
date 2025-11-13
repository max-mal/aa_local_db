import os
import sqlite3
from typing import Optional
from models.file import FileModel
from models.torrent import TorrentFileModel
from repositories.files import FilesRepository
from repositories.torrents import TorrentsRepository


class FilesService:
    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.db = db

        self.torrents_repo = TorrentsRepository(db, cursor)
        self.files_repo = FilesRepository(db, cursor)
        self.torrents_repo = TorrentsRepository(db, cursor)

        from services.torrent import TorrentService
        self.torrents_svc = TorrentService(db, cursor)

    def add_file(self, file: FileModel) -> Optional[int]:
        if file.torrent:
            torrent_id = self.torrents_repo.insert(file.torrent)
            file.torrent_id = torrent_id

        file_id = self.files_repo.insert(file)
        if file_id:
            self.files_repo.insert_fts(file_id, file)
            self.files_repo.link_to_languages(file_id, file.languages)

        return file_id

    def add_to_seeds(self, file: FileModel):
        assert(file.file_id is not None)
        assert(file.torrent_id is not None)

        self.db.execute('BEGIN')

        try:
            filenames = [os.path.basename(path) for path in file.server_path.split(';')]

            self.torrents_svc.seed_torrent(file.torrent_id)
            self.torrents_repo.insert_file(TorrentFileModel(
                torrent_id=file.torrent_id,
                filename=filenames[0],
                file_id=file.file_id
            ))

            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e

    def remove_from_seeds(self, file: FileModel):
        assert(file.file_id is not None)
        assert(file.torrent_id is not None)

        self.db.execute('BEGIN')
        try:
            self.torrents_repo.remove_file(file.file_id)
            count = self.torrents_repo.count_files(file.torrent_id)

            if not count:
                self.torrents_svc.stop_seed_torrent(file.torrent_id)

            self.db.commit()
        except Exception as e:
            self.db.rollback()
