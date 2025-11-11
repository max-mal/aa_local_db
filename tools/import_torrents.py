
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

    def run(self):
        self.db.execute('BEGIN')

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

            self.torrents.upsert(path, magnet_link)
            count += 1

        self.db.commit()
        self.db.close()


if __name__ == '__main__':
    ImportTorrentsTool().run()
