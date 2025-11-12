import os
from typing import List, Optional
from models.file import FileModel
from models.seed import SeedModel
from repositories.files import FilesRepository
from repositories.seeds import SeedsRepository
from repositories.torrents import TorrentsRepository

class FilesService:
    def __init__(self, db, cursor) -> None:
        self.torrents_repo = TorrentsRepository(db, cursor)
        self.files_repo = FilesRepository(db, cursor)
        self.seeds_repo = SeedsRepository(db, cursor)

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
        assert(file.torrent_magnet_link is not None)

        filenames = [os.path.basename(path) for path in file.server_path.split(';')]

        self.seeds_repo.insert(SeedModel(
            file_id=file.file_id,
            filename=filenames[0], # TODO multiple filenames
            magnet_link=file.torrent_magnet_link,
            ipfs_cid=file.ipfs_cid,
        ))
