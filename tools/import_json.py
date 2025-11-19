import sys
import json

from models.file import FileModel
from services.files import FilesService
from utils.db import connect_db


class ImportJsonTool:

    BATCH_SIZE = 1000

    def __init__(self):
        self.db = connect_db()
        self.svc = FilesService(self.db, self.db.cursor())

    def _json_to_model(self, record: dict, type: str):
        source = record.get("_source", {}).get("file_unified_data", {})
        file_md5 = record.get("_source", {}).get("id")[4:]
        server_path = ';'.join(source.get("identifiers_unified", {}).get("server_path", []))
        title = source.get("title_best")
        desc = source.get("stripped_description_best")[:500]
        cover_url = source.get("cover_url_best")
        extension = source.get("extension_best")
        year = source.get("year_best")
        author = source.get("author_best")
        languages = source.get("language_codes", [])
        ipfs_infos = source.get("ipfs_infos", [])
        torrent_paths = source.get("classifications_unified", {}).get("torrent", [])

        ipfs_cid = None
        if isinstance(ipfs_infos, list) and ipfs_infos:
            cids = set([info.get("ipfs_cid") for info in ipfs_infos])
            ipfs_cid = ';'.join(cids)

        return FileModel(
            title=title,
            extension=extension,
            year=year,
            md5=file_md5,
            server_path=server_path,
            description=desc,
            cover_url=cover_url,
            author=author,
            languages=languages,
            ipfs_cid=ipfs_cid,
            torrent=torrent_paths[0] if len(torrent_paths) else None,
            is_journal=True if type == 'journals' else False,
        )

    def run(self, type='books'):
        count = 0
        self.db.execute("BEGIN")  # start transaction

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
                model = self._json_to_model(doc, type)
                self.svc.add_file(model)

                count += 1
                # commit every BATCH_SIZE
                if count % self.BATCH_SIZE == 0:
                    print(count)
                    print("commit!")
                    self.db.commit()
                    self.db.execute("BEGIN")  # start new transaction

            except Exception as e:
                sys.stderr.write(f"Error processing line: {e}\n")
                raise e

        self.db.commit()  # commit remaining records
        self.db.close()


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        type = sys.argv[1]
    else:
        type = 'books'

    ImportJsonTool().run(type)
