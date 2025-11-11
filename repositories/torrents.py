import sqlite3
from typing import Optional

class TorrentsRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

    def insert(self, path: str):
        self.cur.execute("INSERT OR IGNORE INTO torrents (path) VALUES (?)", (path,))
        self.cur.execute("SELECT id FROM torrents WHERE path = ?", (path,))
        row = self.cur.fetchone()
        if row:
            return row[0]

        return None

    def upsert(self, path: str, magnet_link: Optional[str] = None):
        self.cur.execute("SELECT id FROM torrents WHERE path = ?", (path,))
        row = self.cur.fetchone()

        if row:
            self.cur.execute("UPDATE torrents SET magnet_link = ? WHERE id = ?", (magnet_link, row['id']))
        else:
            self.cur.execute(
                "INSERT INTO torrents (path, magnet_link) VALUES (?, ?)",
                (path, magnet_link)
            )

