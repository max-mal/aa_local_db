import sqlite3
from typing import List, Optional

from models.torrent import TorrentFileModel, TorrentModel

class TorrentsRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

    def find_by_magnet_link(self, magnet_link: str):
        sql = "SELECT * FROM torrents WHERE magnet_link = ?"
        self.cur.execute(sql, (magnet_link,))

        row = self.cur.fetchone()
        if row:
            return self._row_to_model(row)

        return None

    def find_by_id(self, torrent_id: int):
        sql = "SELECT * FROM torrents WHERE id = ?"
        self.cur.execute(sql, (torrent_id,))

        row = self.cur.fetchone()
        if row:
            return self._row_to_model(row)

        return None

    def list(self, limit: Optional[int] = None, offset: Optional[int] = None):
        sql = "SELECT * FROM torrents"
        params = []

        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        if offset:
            sql += " OFFSET ?"
            params.append(offset)

        self.cur.execute(sql, params)
        return [
            self._row_to_model(row)
            for row in self.cur.fetchall()
        ]

    def list_seeding(self):
        sql = "SELECT * FROM torrents WHERE is_seeding = 1"

        self.cur.execute(sql)
        return [
            self._row_to_model(row)
            for row in self.cur.fetchall()
        ]

    def insert(self, path: str):
        self.cur.execute("INSERT OR IGNORE INTO torrents (path) VALUES (?)", (path,))
        self.cur.execute("SELECT id FROM torrents WHERE path = ?", (path,))
        row = self.cur.fetchone()
        if row:
            return row[0]

        return None

    def upsert(self, model: TorrentModel):
        self.cur.execute("SELECT id FROM torrents WHERE path = ?", (model.path,))
        row = self.cur.fetchone()

        if row:
            self.cur.execute(
                """
                UPDATE torrents SET
                    magnet_link = ?,
                    added_to_torrents_list_at = ?,
                    data_size = ?,
                    obsolete = ?,
                    embargo = ?,
                    num_files = ?,
                    is_seeding = ?,
                    is_seed_all = ?
                WHERE id = ?
                """,
                (
                    model.magnet_link,
                    model.added_to_torrents_list_at,
                    model.data_size,
                    model.obsolete,
                    model.embargo,
                    model.num_files,
                    model.is_seeding,
                    model.is_seed_all,
                    row['id']
                )
            )
        else:
            self.cur.execute(
                """
                INSERT INTO torrents (
                    path,
                    magnet_link,
                    added_to_torrents_list_at,
                    data_size,
                    obsolete,
                    embargo,
                    num_files,
                    is_seeding,
                    is_seed_all,
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model.path,
                    model.magnet_link,
                    model.added_to_torrents_list_at,
                    model.data_size,
                    model.obsolete,
                    model.embargo,
                    model.num_files,
                    model.is_seeding,
                    model.is_seed_all,
                )
            )
            return self.cur.lastrowid

    def _row_to_model(self, row):
        return TorrentModel(
            path=row['path'],
            torrent_id=row['id'],
            magnet_link=row['magnet_link'],
            added_to_torrents_list_at=row['added_to_torrents_list_at'],
            data_size=row['data_size'],
            num_files=row['num_files'],
            embargo=bool(row['embargo']) if row['embargo'] is not None else None,
            obsolete=bool(row['obsolete']) if row['obsolete'] is not None else None,
            is_seeding=row['is_seeding'] == 1,
            is_seed_all=row['is_seed_all'] == 1,
        )

    # Torrent file methods

    def list_files(self, torrent_ids: List[int]):
        sql = f"""
        SELECT tf.id, tf.torrent_id, tf.filename, tf.file_id, tf.is_complete, tf.local_path, f.byteoffset FROM torrent_files tf
            LEFT JOIN files f on tf.file_id = f.id
            WHERE tf.torrent_id IN ({','.join(['?'] * len(torrent_ids))})
        """
        self.cur.execute(sql, torrent_ids)

        return [
            TorrentFileModel(
                torrent_file_id=row['id'],
                torrent_id=row['torrent_id'],
                filename=row['filename'],
                file_id=row['file_id'],
                is_complete=row['is_complete'],
                local_path=row['local_path'],
                byteoffset=row['byteoffset'],
            )
            for row in self.cur.fetchall()
        ]

    def count_files(self, torrent_id):
        self.cur.execute(
            """
            SELECT count(*) as count FROM torrent_files WHERE torrent_id = ?
            """, (torrent_id,)
        )

        row = self.cur.fetchone()
        if row:
            return row['count']

        return 0

    def set_file_complete(self, torrent_file_id: int, local_path: Optional[str]):
        self.cur.execute(
            "UPDATE torrent_files SET is_complete = 1, local_path = ? WHERE id = ?",
            (local_path, torrent_file_id, )
        )

    def remove_file(self, file_id: int):
        self.cur.execute("DELETE FROM torrent_files WHERE file_id = ?", (file_id,))
        return self.cur.rowcount

    def insert_file(self, model: TorrentFileModel):
        self.cur.execute("""
            INSERT OR IGNORE INTO torrent_files (
                torrent_id,
                filename,
                file_id,
                is_complete,
                local_path
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                model.torrent_id,
                model.filename,
                model.file_id,
                model.is_complete,
                model.local_path
            )
        )
        return self.cur.lastrowid


