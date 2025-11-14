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

    def find_by_ids(self, ids: List[int]):
        sql = f"""
        SELECT f.*, t.path AS torrent_path, t.magnet_link as torrent_magnet_link,
            tf.is_complete as is_complete
        FROM files f
        LEFT JOIN torrents t ON t.id = f.torrent_id
        LEFT JOIN torrent_files tf ON f.id = tf.file_id
        WHERE f.id IN ({','.join([str(id) for id in ids])})
        """

        self.cur.execute(sql)

        results: List[FileModel]
        results = []
        for row in self.cur.fetchall():
            model = self._row_to_model(row)

            model.load_description(row['description_compressed'])
            results.append(model)

        return results

    def search(
        self,
        query_text=None,
        language=None,
        year=None,
        torrent_id=None,
        local_only=False,
        limit=50,
        offset=0,
        order_by=None
    ):
        sql = """
        SELECT f.*, t.path AS torrent_path, t.magnet_link as torrent_magnet_link,
            tf.is_complete as is_complete
        FROM files f
        LEFT JOIN torrents t ON t.id = f.torrent_id
        """

        filters = []
        params = []
        if query_text:
            sql += " JOIN files_fts ON files_fts.rowid = f.id"
            filters.append("files_fts MATCH ?")
            params.append(query_text)

        if language:
            sql += " JOIN file_languages fl ON fl.file_id = f.id"
            filters.append("fl.language_code = ?")
            params.append(language)

        if local_only:
            sql += " INNER JOIN torrent_files tf ON f.id = tf.file_id"
        else:
            sql += " LEFT JOIN torrent_files tf ON f.id = tf.file_id"

        if year:
            filters.append("f.year = ?")
            params.append(year)

        if torrent_id:
            filters.append("f.torrent_id = ?")
            params.append(torrent_id)

        if filters:
            sql += " WHERE " + " AND ".join(filters)

        if order_by:
            sql += f" ORDER BY {order_by} NULLS FIRST"

        sql += " LIMIT ? OFFSET ?"
        params.append(limit)
        params.append(offset)

        self.cur.execute(sql, params)

        results: List[FileModel]
        results = []
        for row in self.cur.fetchall():
            model = self._row_to_model(row)

            model.load_description(row['description_compressed'])
            results.append(model)

        return results

    def _row_to_model(self, row):
        model = FileModel(
            file_id=row['id'],
            md5=row['md5'],
            server_path=row['server_path'],
            ipfs_cid=row['ipfs_cid'],
            torrent=row['torrent_path'],
            torrent_id=row['torrent_id'],
            torrent_magnet_link=row['torrent_magnet_link'],
            title=row['title'],
            cover_url=row['cover_url'],
            extension=row['extension'],
            year=row['year'],
            author=row['author'],
            is_complete=row['is_complete'],
        )

        return model
