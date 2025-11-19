from typing import Dict, List
from models.torrent import TorrentFileModel, TorrentModel
from repositories.files import FilesRepository
from repositories.torrents import TorrentsRepository


class TorrentService:
    def __init__(self, db, cursor) -> None:
        self.db = db

        self.torrents_repo = TorrentsRepository(db, cursor)
        self.files_repo = FilesRepository(db, cursor)

    def seed_torrent(self, torrent_id: int, seed_all=None):
        torrent = self.torrents_repo.find_by_id(torrent_id)
        if not torrent:
            raise Exception("torrent not found")

        torrent.is_seeding = True
        torrent.is_seed_all = seed_all if seed_all is not None else False
        self.torrents_repo.upsert(torrent)

    def stop_seed_torrent(self, torrent_id: int):
        torrent = self.torrents_repo.find_by_id(torrent_id)
        if not torrent:
            raise Exception("torrent not found")

        torrent.is_seeding = False
        self.torrents_repo.upsert(torrent)

    def populate_files(self, models: List[TorrentModel]):
        filtered = filter(lambda t: not t.is_seed_all, models)
        ids = list(map(lambda t: t.torrent_id or -1, filtered))

        files = self.torrents_repo.list_files(ids)

        files_by_id: Dict[int, List[TorrentFileModel]]
        files_by_id = {}
        for f in files:
            if f.torrent_id not in files_by_id:
                files_by_id[f.torrent_id] = []

            files_by_id[f.torrent_id].append(f)

        for model in models:
            assert(model.torrent_id)
            model.files = files_by_id.get(model.torrent_id, [])

        return models

    def list_seeding(self):
        models = self.torrents_repo.list_seeding()

        self.populate_files(models)
        return models
