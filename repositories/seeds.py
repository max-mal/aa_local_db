import sqlite3
from typing import Optional

from models.seed import SeedModel

class SeedsRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

    def list(self):
        self.cur.execute("SELECT * FROM seeds")
        return [
            SeedModel(
                seed_id=row['id'],
                file_id=row['file_id'],
                filename=row['filename'],
                magnet_link=row['magnet_link'],
                ipfs_cid=row['ipfs_cid'],
            )
            for row in self.cur.fetchall()
        ]

    def insert(self, model: SeedModel):
        self.cur.execute(
            "INSERT INTO seeds (file_id, filename, magnet_link, ipfs_cid) VALUES (?, ?, ?, ?)",
            (model.file_id, model.filename, model.magnet_link, model.ipfs_cid)
        )
        return self.cur.lastrowid

    def remove(self, seed_id: int):
        self.cur.execute("DELETE FROM seeds WHERE id = ?", (seed_id,))
        return self.cur.rowcount
