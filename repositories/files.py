import sqlite3
from typing import List
from models.file import FileModel

class FilesRepository:
    def __init__(self, conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
        self.conn = conn
        self.cur = cursor

    def insert(self, file: FileModel):
        self.cur.execute("""
            INSERT OR IGNORE INTO files (
                md5,
                title,
                description_compressed,
                cover_url,
                extension,
                year,
                author,
                ipfs_cid,
                torrent_id,
                server_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file.md5,
            file.title,
            file.get_description_compressed(),
            file.cover_url,
            file.extension,
            file.year,
            file.author,
            file.ipfs_cid,
            file.torrent_id,
            file.server_path)
        )

        file_id = self.cur.lastrowid
        if file_id:
            self.cur.execute("""
                INSERT INTO files_fts (rowid, title, author)
                VALUES (?, ?, ?)
            """, (file_id, file.title, file.author))

        return file_id

    def insert_fts(self, file_id: int, file: FileModel):
        self.cur.execute("""
            INSERT INTO files_fts (rowid, title, author)
            VALUES (?, ?, ?)
        """, (file_id, file.title, file.author))

    def link_to_languages(self, file_id: int, languages: List[str]):
        for language in languages:
            self.cur.execute("INSERT INTO file_languages (file_id, language_code) VALUES (?, ?)", (file_id, language))


    def list_languages(self):
        self.cur.execute(
            "SELECT language_code FROM file_languages GROUP by language_code;"
        )

        return [row['language_code'] for row in self.cur.fetchall()]

    def search(self, query_text, language=None, year=None, limit=50, offset=0, order_by='rank ASC'):
        sql = """
        SELECT f.*, t.path AS torrent_path, t.magnet_link as torrent_magnet_link
        FROM files f
        JOIN files_fts ON files_fts.rowid = f.id
        LEFT JOIN torrents t ON t.id = f.torrent_id
        """

        filters = []
        params = []
        if query_text:
            filters.append("files_fts MATCH ?")
            params.append(query_text)

        if language:
            sql += " JOIN file_languages fl ON fl.file_id = f.id"
            filters.append("fl.language_code = ?")
            params.append(language)

        if year:
            filters.append("f.year = ?")
            params.append(year)

        if filters:
            sql += " WHERE " + " AND ".join(filters)

        sql += f"ORDER BY {order_by} LIMIT ? OFFSET ?"
        params.append(limit)
        params.append(offset)

        self.cur.execute(sql, params)

        results = []
        for row in self.cur.fetchall():
            model = FileModel(
                file_id=row['id'],
                md5=row['md5'],
                server_path=row['server_path'],
                ipfs_cid=row['ipfs_cid'],
                torrent=row['torrent_path'],
                torrent_magnet_link=row['torrent_magnet_link'],
                title=row['title'],
                cover_url=row['cover_url'],
                extension=row['extension'],
                year=row['year'],
                author=row['author'],
            )

            model.load_description(row['description_compressed'])
            results.append(model)

        return results
