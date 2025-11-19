Project is designed to be a simplified local browser / downloader for annas archive

DB populated by books only takes ~33GB

Data stored in sqlite3 database

Amount of data is reduced by storing only:
- md5
- server_path
- title
- zlib-compressed first 500 chars of description
- cover_url
- extension
- year
- author
- ipfs_cid
- link to torrent

Usage:

- Install requirements `pip install -r requirements.txt`
- Copy `config.py.example` to `config.py`, edit for your environment
- Import torrents records `python3 -m tools.import_torrents`
- Download and import books metadata `python3 -m tools.import_download`
- Import byteoffsets data `zstdcat annas_archive_meta__aacid__torrents_byteoffsets_records__20250712T225427Z--20250712T225427Z.jsonl.seekable.zst | python3 -m tools.import_byteoffsets`

- Run web UI `streamlit run streamlit_app.py`
