import sys
import orjson
import os
import multiprocessing as mp

from models.file import FileModel
from services.files import FilesService
from utils.db import connect_db


class ImportJsonTool:

    BATCH_SIZE = 1000

    def __init__(self):
        self.should_exit = mp.Value('b', False)

    def _json_to_model(self, record: dict, type: str):
        source = record.get("_source", {}).get("file_unified_data", {})
        file_md5 = record.get("_source", {}).get("id")[4:]
        server_path = ';'.join(source.get("identifiers_unified", {}).get("server_path", []))
        title = source.get("title_best")

        if title is None:
            filename_additional = source.get('original_filename_additional', [])
            if len(filename_additional):
                filename = filename_additional[0]
                title = os.path.basename(filename.replace("\\", "/"))

        if title is None:
            original_filename_best = source.get('original_filename_best')
            if original_filename_best:
                title = original_filename_best

        desc = source.get("stripped_description_best")[:500]
        cover_url = source.get("cover_url_best")
        extension = source.get("extension_best")
        year = source.get("year_best")
        author = source.get("author_best")

        if author is None:
            edition_varia_best = source.get('edition_varia_best')
            if edition_varia_best:
                author = edition_varia_best

        languages = source.get("language_codes", [])
        ipfs_infos = source.get("ipfs_infos", [])
        torrent_paths = source.get("classifications_unified", {}).get("torrent", [])

        ipfs_cid = None
        if isinstance(ipfs_infos, list) and ipfs_infos:
            cids = set([info.get("ipfs_cid") for info in ipfs_infos])
            ipfs_cid = ';'.join(cids)

        model = FileModel(
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
        model.set_description_compressed()

        return model

    def add_file_worker(self, queue: mp.Queue):
        db = connect_db()
        svc = FilesService(db, db.cursor())

        db.execute('PRAGMA synchronous = 0')
        db.execute("BEGIN")  # start transaction

        svc.populate_torrents_cache()

        count = 0
        while not self.should_exit.value:
            model: FileModel
            model = queue.get()

            svc.add_file(model)

            count += 1
            # commit every BATCH_SIZE
            if count % self.BATCH_SIZE == 0:
                print(count)
                db.commit()
                db.execute("BEGIN")  # start new transaction

        db.commit()
        db.close()


    def run(self, type='books'):
        file_add_queue: mp.Queue[FileModel]
        file_add_queue = mp.Queue(self.BATCH_SIZE * 2)

        file_add_process = mp.Process(target=self.add_file_worker, args=(file_add_queue,))
        file_add_process.start()

        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                doc = orjson.loads(line)
                model = self._json_to_model(doc, type)

                # Skip not downloadable files
                if not model.torrent and not model.ipfs_cid:
                    continue

                file_add_queue.put(model)

            except Exception as e:
                sys.stderr.write(f"Error processing line: {e}\n")
                raise e

        self.should_exit.value = True
        print("Waiting for file_add_process...")
        file_add_process.join()
        print("Done")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        type = sys.argv[1]
    else:
        type = 'books'

    ImportJsonTool().run(type)
