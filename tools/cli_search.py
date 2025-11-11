import textwrap

from models.file import FileModel
from services.files import FilesService
from utils.db import connect_db


class CliSearchTool:
    def __init__(self) -> None:
        db = connect_db()
        self.svc = FilesService(db, db.cursor())


    def print_result(self, file: FileModel):
        if file.ipfs_cid:
            ipfs_urls = [f"https://ipfs.io/ipfs/{cid}" for cid in set(file.ipfs_cid.split(';'))]
        else:
            ipfs_urls = []

        print("=" * 80)
        print(f"Title       : {file.title}")
        print(f"Author      : {file.author or '-'}")
        print(f"Year        : {file.year}")

        if file.torrent:
            print(f"Torrent     : https://annas-archive.org/dyn/small_file/torrents/{file.torrent}")

        for url in ipfs_urls:
            print(f"IPFS        : {url}")

        print(f"Cover       : {file.cover_url or '-'}")
        print(f"md5         : {file.md5}")
        print(f"server_path : {file.server_path}")

        if file.description:
            print("Description :")
            wrapped = textwrap.fill(file.description, width=78, initial_indent='  ', subsequent_indent='  ')
            print(wrapped)

        print("=" * 80)
        print()  # extra newline

    def run(self):
        while True:
            query = input('Search (q to quit): ').strip()
            if query.lower() == 'q':
                break

            results = self.svc.search(query)
            if not results:
                print("No results found.\n")
                continue

            for r in results:
                self.print_result(r)


if __name__ == '__main__':
    CliSearchTool().run()
