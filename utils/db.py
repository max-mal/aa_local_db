#!/usr/bin/env python3
import sqlite3

DB_FILE = "/mnt/D42/data.db"

def connect_db():
    conn = sqlite3.connect(DB_FILE, isolation_level=None)
    conn.row_factory = sqlite3.Row

    init_db(conn)
    return conn

# --- Database setup ---
def init_db(conn):
    cur = conn.cursor()

    # Main files table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        md5 TEXT UNIQUE,
        server_path TEXT,
        title TEXT,
        description_compressed BLOB,
        cover_url TEXT,
        extension TEXT,
        year INTEGER,
        author TEXT,
        ipfs_cid TEXT,
        torrent_id INTEGER,
        FOREIGN KEY(torrent_id) REFERENCES torrents(id)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_year ON files(year);")

    # FTS table for searchable text fields
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
        title,
        author,
        content='files',
        content_rowid='id'
    );
    """)

    # Languages table (many-to-one)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_languages (
        file_id INTEGER,
        language_code TEXT,
        FOREIGN KEY(file_id) REFERENCES files(id)
    );
    """)

    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_languages_file_id ON file_languages(file_id);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_file_languages_language_code ON file_languages(language_code);"
    )

    # Torrents table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS torrents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT UNIQUE,
        magnet_link TEXT
    );
    """)

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_torrents_path ON torrents(path);"
    )

    conn.commit()


if __name__ == "__main__":
    conn = connect_db()
    init_db(conn)

