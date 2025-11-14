from os import unlink
import subprocess
import glob
import sys
from repositories.aa_torrents import AnnasArchiveTorrentsRepository
from utils.torrent import TorrentDownloader, FileNotFoundException
from config import DOWNLOADS_DIR

class ImportDownloadTool:
    def __init__(self):
        pass

    def find_torrent(self):
        aa_torrents = AnnasArchiveTorrentsRepository()
        torrents_list = aa_torrents.list()

        def filter_func(t):
            if t.get('group_name') != 'aa_derived_mirror_metadata':
                return False

            if t.get('embargo'):
                return False

            return True

        metadata_torrents = list(
            filter(filter_func, torrents_list)
        )

        metadata_torrents.sort(
            key=lambda t: t.get('added_to_torrents_list_at'),
            reverse=True
        )

        return metadata_torrents[0]

    def run(self, torrent_or_magnet=None):
        if not torrent_or_magnet:
            torrent = self.find_torrent()
            torrent_or_magnet = torrent.get('magnet_link')

        downloader = TorrentDownloader(downloads_dir=DOWNLOADS_DIR)

        index = int(sys.argv[1]) if len(sys.argv) > 1 else 0

        while True:
            filename = f"aarecords__{index}.json.gz"
            try:
                downloader.download(torrent_or_magnet, filename)
            except FileNotFoundException:
                break

            paths = glob.glob(f'**/{filename}', recursive=True)

            ret = subprocess.call(['bash', '-c', f"zcat '{paths[0]}' | python3 -m tools.import_json"])
            if ret != 0:
                raise Exception("Import failed")

            print("import done", filename)
            unlink(paths[0])
            index += 1


if __name__ == '__main__':
    ImportDownloadTool().run()
