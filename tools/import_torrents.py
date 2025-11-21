
import json
from typing import Optional
from models.torrent import TorrentModel
from repositories.aa_torrents import AnnasArchiveTorrentsRepository
from repositories.torrents import TorrentsRepository
from utils.db import connect_db


class ImportTorrentsTool:

    BATCH_SIZE = 100

    def __init__(self):
        self.db = connect_db()
        cursor = self.db.cursor()

        self.aa_torrents = AnnasArchiveTorrentsRepository()
        self.torrents = TorrentsRepository(self.db, cursor)

    def run(self, source_path: Optional[str] = None):
        self.db.execute('BEGIN')

        if source_path:
            with open(source_path, 'r') as f:
                torrents = json.load(f)
        else:
            torrents = self.aa_torrents.list()

        print("Have torrents:", len(torrents))

        count = 0
        for torrent in torrents:
            if count % self.BATCH_SIZE == 0:
                print(count)
                self.db.commit()
                self.db.execute("BEGIN")

            path = torrent.get('url', '').replace(self.aa_torrents.FILE_URL, '')
            magnet_link = torrent.get('magnet_link')
            added_to_torrents_list_at = torrent.get('added_to_torrents_list_at')
            data_size = torrent.get('data_size')
            obsolete = torrent.get('obsolete')
            embargo = torrent.get('embargo')
            num_files = torrent.get('num_files')

            self.torrents.upsert(TorrentModel(
                path=path,
                magnet_link=magnet_link,
                added_to_torrents_list_at=added_to_torrents_list_at,
                data_size=data_size,
                obsolete=obsolete,
                embargo=embargo,
                num_files=num_files,
            ))
            count += 1

        self.db.commit()
        self.db.close()


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        path = None

    ImportTorrentsTool().run(path)
