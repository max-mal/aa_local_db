from dataclasses import dataclass
import json
import sqlite3
import sys
from typing import Dict
from models.file import FileModel
from models.torrent import TorrentModel
from repositories.aa_torrents import AnnasArchiveTorrentsRepository
from repositories.files import FilesRepository
from torrent_byteoffset_dl import calculate_tar_end_offset, calculate_zip_end_offset, decode_tar_header, decode_zip_header, find_tar_header, find_zip_header, get_zip_compressed_data
from utils.db import connect_db
from utils.helpers import infohash_from_magnet
from config import DOWNLOADS_DIR
import os
import libtorrent as lt

@dataclass
class ByteoffsetInfo:
    path: str
    start_offset: int

class ByteoffsetFileExtractor:
    def __init__(self, db: sqlite3.Connection, cursor: sqlite3.Cursor):
        self.db = db
        self.cursor = cursor

        from repositories.torrents import TorrentsRepository
        self.torrents_repo = TorrentsRepository(db, cursor)
        self.aa_torrents_repo = AnnasArchiveTorrentsRepository()

    def extract(self, file: FileModel):
        torrent = self.torrents_repo.find_by_id(file.torrent_id)
        if not torrent:
            return None

        infos = self.read_byteoffsets_file(torrent.magnet_link)
        if infos:
            if info := infos.get(file.byteoffset):
                return self.extract_file(info, file.byteoffset)

        info = self.get_info_from_torrent(torrent, file.byteoffset)
        return self.extract_file(info, file.byteoffset)

    def extract_file(self, info: ByteoffsetInfo, offset: int):
        file_path = os.path.join(DOWNLOADS_DIR, info.path)
        with open(file_path, 'rb') as f:
            # start reading 512 bytes preceeding byteoffset
            # zip header takes ~100bytes, tar 512 bytes exactly
            # while byteoffset points to start of data (not header) in torrent
            start = offset - info.start_offset - 512
            f.seek(start)
            data = f.read(512)

            if info.path.endswith('.zip'):
                pt = find_zip_header(data, start, offset)
                if pt is None:
                    raise Exception("Header not found")

                header = decode_zip_header(data, pt)
                data_end = calculate_zip_end_offset(header, start, pt)
                data_start = data_end - header.comp_size

            elif info.path.endswith('.tar'):
                pt = find_tar_header(data, start, offset)
                if pt is None:
                    raise Exception("Header not found")

                header = decode_tar_header(data, pt)
                data_end = calculate_tar_end_offset(header, start, pt)
                data_start = data_end - header.filesize
            else:
                raise Exception("Unknown container")


            f.seek(data_start)
            return f.read(data_end - data_start)

    def read_byteoffsets_file(self, magnet_link) -> Dict[int, ByteoffsetInfo]:
        infohash = infohash_from_magnet(magnet_link)

        path = os.path.join(DOWNLOADS_DIR, f'{infohash}_byteoffsets.json')
        if not os.path.exists(path):
            return None

        with open(path, 'r') as f:
            data = json.load(f)

        result: Dict[int, ByteoffsetInfo]
        result = {}

        for key in data.keys():
            result[int(key)] = ByteoffsetInfo(
                path=data[key]['path'],
                start_offset=data[key]['start_offset'],
            )

        return result

    def get_info_from_torrent(self, torrent: TorrentModel, byteoffset: int):
        infohash = infohash_from_magnet(torrent.magnet_link)

        torrent_path = os.path.join(DOWNLOADS_DIR, f'{infohash}.torrent')
        if not os.path.exists(torrent_path):
            data = self.aa_torrents_repo.get_one(torrent.path)
            with open(torrent_path, 'wb') as f:
                f.write(data)

        ti = lt.torrent_info(torrent_path)
        ti_files = ti.files()

        result: Dict[int, ByteoffsetInfo]
        result = {}

        file_index = ti_files.file_index_at_offset(byteoffset)
        path = ti_files.file_path(file_index)
        offset = ti_files.file_offset(file_index)

        return ByteoffsetInfo(path=path, start_offset=offset)


if __name__ == '__main__':
    db = connect_db()
    cursor = db.cursor()

    files_repo = FilesRepository(db, cursor)

    files = files_repo.find_by_ids([sys.argv[1]])
    if not len(files):
        print("File not found")
        exit()

    file = files[0]

    extractor = ByteoffsetFileExtractor(db, cursor)
    data = extractor.extract(file)
    if data is None:
        print("Extraction failed")
        exit()

    filename = f'{file.md5}.{file.extension}'
    with open(filename, 'wb') as f:
        f.write(data)

    print("Extracted", filename)
