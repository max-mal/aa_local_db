import sys
from typing import List, Optional
import os
import multiprocessing as mp

from models.file import FileModel
from services.files import FilesService
from utils.db import connect_db

from msgspec.json import decode
from msgspec import Struct


class IdentifiersUnified(Struct):
    server_path: List[str] = []

class ClassificationsUnified(Struct):
    torrent: List[str] = []

class IpfsInfo(Struct):
    ipfs_cid: str

class FileUnifiedData(Struct):
    title_best: Optional[str]
    original_filename_best: Optional[str]

    stripped_description_best: str
    cover_url_best: Optional[str]
    extension_best: str
    year_best: str
    author_best: Optional[str]
    edition_varia_best: Optional[str]
    language_codes: List[str]

    classifications_unified: ClassificationsUnified
    identifiers_unified: IdentifiersUnified

    ipfs_infos: Optional[List[IpfsInfo]] = []

    original_filename_additional: List[str] = []


class JsonDocSource(Struct):
    id: str
    file_unified_data: FileUnifiedData


class JsonDoc(Struct):
    _source: JsonDocSource


class ImportJsonTool:

    BATCH_SIZE = 1000

    def __init__(self):
        self.should_exit = mp.Value('b', False)

    def json_to_model(self, record: JsonDoc, type: str):
        source = record._source
        file_data = source.file_unified_data

        file_md5 = source.id[4:]
        server_path = ';'.join(file_data.identifiers_unified.server_path)
        title = file_data.title_best

        if title is None:
            filename_additional = file_data.original_filename_additional
            if len(filename_additional):
                filename = filename_additional[0]
                title = os.path.basename(filename.replace("\\", "/"))

        if title is None:
            original_filename_best = file_data.original_filename_best
            if original_filename_best:
                title = original_filename_best

        desc = file_data.stripped_description_best[:500]
        cover_url = file_data.cover_url_best
        extension = file_data.extension_best
        year = file_data.year_best

        author = file_data.author_best

        if author is None:
            edition_varia_best = file_data.edition_varia_best
            if edition_varia_best:
                author = edition_varia_best

        languages = file_data.language_codes
        ipfs_infos = file_data.ipfs_infos
        torrent_paths = file_data.classifications_unified.torrent

        ipfs_cid = None
        if isinstance(ipfs_infos, list) and ipfs_infos:
            cids = set([info.ipfs_cid for info in ipfs_infos])
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
        while not self.should_exit.value or not queue.empty():
            model: FileModel
            model = queue.get()

            svc.add_file(model)

            count += 1
            # commit every BATCH_SIZE
            if count % self.BATCH_SIZE == 0:
                print(count)
                db.commit()
                db.execute("BEGIN")  # start new transaction

        print('commiting and closing data')
        db.commit()
        db.close()
        print("add_file_worker complete")


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
                doc = decode(line, type=JsonDoc)
                # doc = orjson.loads(line)
                model = self.json_to_model(doc, type)

                # Skip not downloadable files
                if not model.torrent and not model.ipfs_cid:
                    continue

                file_add_queue.put(model)
            except Exception as e:
                sys.stderr.write(f"Error processing line: {e}\n")
                raise e

        self.should_exit.value = True
        file_add_queue.close()

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
