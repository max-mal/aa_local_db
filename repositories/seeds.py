import sqlite3
from typing import Optional

from models.seed import SeedModel

class SeedsRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

    def list(self, limit=None, offset=None):
        sql = "SELECT * FROM seeds ORDER BY id DESC"
        params = []

        if limit:
            sql += " LIMIT ?"
            params.append(limit)

        if offset:
            sql += " OFFSET ?"
            params.append(offset)

        self.cur.execute(sql, params)

        return [
            SeedModel(
                seed_id=row['id'],
                file_id=row['file_id'],
                filename=row['filename'],
                magnet_link=row['magnet_link'],
                ipfs_cid=row['ipfs_cid'],
                is_complete=row['is_complete'] == 1,
                path=row['path'],
            )
            for row in self.cur.fetchall()
        ]

    def count_by_magnets(self):
        self.cur.execute(
        """
        SELECT magnet_link, count(*) as count from seeds GROUP BY magnet_link
        """)

        return [
            {"magnet_link": row['magnet_link'], "count": row['count']}
            for row in self.cur.fetchall()
        ]

    def insert(self, model: SeedModel):
        self.cur.execute(
            "INSERT OR IGNORE INTO seeds (file_id, filename, magnet_link, ipfs_cid) VALUES (?, ?, ?, ?)",
            (model.file_id, model.filename, model.magnet_link, model.ipfs_cid)
        )
        return self.cur.lastrowid

    def remove(self, seed_id: int):
        self.cur.execute("DELETE FROM seeds WHERE id = ?", (seed_id,))
        return self.cur.rowcount

    def set_complete(self, seed_id: int, path: Optional[str] = None):
        self.cur.execute(
            "UPDATE seeds SET is_complete = 1, path = ? WHERE id = ?",
            (path, seed_id, )
        )
        self.conn.commit()
