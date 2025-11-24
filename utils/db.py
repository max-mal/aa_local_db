#!/usr/bin/env python3
import sqlite3
import time
from config import DB_FILE

def connect_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL")

    init_db(conn)
    return conn


def interrupt_after(seconds, connection):
    start = time.time()
    def progress():
        if time.time() - start > seconds:
            return 1  # nonzero return = abort query
    connection.set_progress_handler(progress, 1000)

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
        year TEXT,
        author TEXT,
        language TEXT,
        ipfs_cid TEXT,
        torrent_id INTEGER,
        byteoffset integer,
        is_journal int DEFAULT 0 NOT NULL,
        FOREIGN KEY(torrent_id) REFERENCES torrents(id)
    );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_year ON files(year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_torrent_id ON files(torrent_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_files_is_journal on files(is_journal);")

    # FTS table for searchable text fields
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
        text,
        content=''
    );
    """)

    # Torrents table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS torrents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT UNIQUE,
        magnet_link TEXT,
        added_to_torrents_list_at TEXT,
        data_size INT,
        obsolete INT,
        embargo INT,
        num_files INT,
        is_seeding INT NOT NULL DEFAULT 0,
        is_seed_all INT NOT NULL DEFAULT 0
    );
    """)

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_torrents_path ON torrents(path);"
    )

    cur.execute("""
    CREATE TABLE IF NOT EXISTS "torrent_files" (
        id INTEGER,
        torrent_id INTEGER NOT NULL,
        filename TEXT,
        file_id INT NOT NULL,
        is_complete INT NOT NULL DEFAULT 0,
        local_path TEXT,
        PRIMARY KEY("id" AUTOINCREMENT)
    );
    """)

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_torrent_files_file_id ON torrent_files(file_id);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_torrent_files_torrent_id ON torrent_files(torrent_id);"
    )

    conn.commit()


if __name__ == "__main__":
    conn = connect_db()
    init_db(conn)

