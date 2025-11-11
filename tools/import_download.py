from os import unlink
import subprocess
import glob
import sys
from utils.torrent import TorrentDownloader, FileNotFoundException


class ImportDownloadTool:
    def __init__(self):
        pass

    def run(self):
        downloader = TorrentDownloader()

        source = 'magnet:?xt=urn:btih:8b28482be52c17dd8a9cb4d64bddf98d5e2191fa&dn=aa_derived_mirror_metadata_20250904.torrent&tr=udp://tracker.opentrackr.org:1337/announce'
        index = int(sys.argv[1]) if len(sys.argv) > 1 else 0

        while True:
            filename = f"aarecords__{index}.json.gz"
            try:
                downloader.download(source, filename)
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
