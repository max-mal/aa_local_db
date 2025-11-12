import sqlite3
from typing import Optional

from models.torrent import TorrentModel

class TorrentsRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

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
            TorrentModel(
                path=row['path'],
                torrent_id=row['id'],
                magnet_link=row['magnet_link'],
                added_to_torrents_list_at=row['added_to_torrents_list_at'],
                data_size=row['data_size'],
                num_files=row['num_files'],
                embargo=bool(row['embargo']) if row['embargo'] is not None else None,
                obsolete=bool(row['obsolete']) if row['obsolete'] is not None else None
            )
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
                    num_files = ?
                WHERE id = ?
                """,
                (
                    model.magnet_link,
                    model.added_to_torrents_list_at,
                    model.data_size,
                    model.obsolete,
                    model.embargo,
                    model.num_files,
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
                    num_files
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model.path,
                    model.magnet_link,
                    model.added_to_torrents_list_at,
                    model.data_size,
                    model.obsolete,
                    model.embargo,
                    model.num_files,
                )
            )
            return self.cur.lastrowid

