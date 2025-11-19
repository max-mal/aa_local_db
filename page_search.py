import sqlite3
import streamlit as st
import textwrap
from models.file import FileModel
from models.torrent import TorrentFileModel
from repositories.aa_torrents import AnnasArchiveTorrentsRepository
from repositories.files import FilesRepository
from repositories.torrents import TorrentsRepository
from services.files import FilesService
from services.torrent import TorrentService
from utils.byteoffset_extract import ByteoffsetFileExtractor
from utils.db import connect_db, interrupt_after
from utils.torrent import TorrentDownloader
import os
import glob

from streamlit.components.v1 import html
from config import DOWNLOADS_DIR, UI_IPFS_GATEWAY

# Initialize DB and services
db = connect_db()
cursor = db.cursor()
svc = FilesService(db, cursor)
repo = FilesRepository(db, cursor)
extractor = ByteoffsetFileExtractor(db, cursor)

interrupt_after(15, db)

aa_torrents = AnnasArchiveTorrentsRepository()


def get_file_path(file: FileModel):
    server_paths = [os.path.basename(p) for p in file.server_path.split(';')]
    # TODO use local_path from torrent_files

    # if seed.path:
    #     return f"{downloads_dir}/{seed.path}"
    for sp in server_paths:
        paths = glob.glob(f"{DOWNLOADS_DIR}/**/{sp}", recursive=True)
        if len(paths):
            return paths[0]

    return None

@st.cache_data(ttl=120)
def search(
    query,
    search_lang,
    search_year,
    search_torrent_id,
    limit,
    offset,
    sort,
    sort_direction,
    local_only=False,
):
    order_by = 'rank'

    if sort == 'year':
        order_by = 'year'
    elif sort == 'title':
        order_by = 'title'
    elif sort == 'none':
        order_by = None

    if order_by == 'rank' and not query:
        order_by = None

    if order_by:
        if sort_direction == 'ascending':
            order_by += ' ASC'
        else:
            order_by += ' DESC'

    try:
        return repo.search(
            query_text=query,
            language=search_lang,
            year=search_year,
            torrent_id=search_torrent_id,
            limit=limit,
            offset=offset,
            order_by=order_by,
            local_only=local_only,
        )
    except sqlite3.OperationalError as e:
        if query:
            query = '"' + query.replace('"', '""') + '"*'
            return repo.search(
                query_text=query,
                language=search_lang,
                year=search_year,
                torrent_id=search_torrent_id,
                limit=limit,
                offset=offset,
                order_by=order_by,
                local_only=local_only,
            )

def seed_file(file: FileModel, container):
    db = connect_db()
    cursor = db.cursor()

    svc = FilesService(db, cursor)
    svc.add_to_seeds(file)
    db.commit()

    with container:
        st.success("File added to seeds")
        st.cache_data.clear()


def seed_torrent(torrent_id, container):
    db = connect_db()
    cursor = db.cursor()

    torrent_svc = TorrentService(db, cursor)
    torrent_svc.seed_torrent(torrent_id, seed_all=True)
    db.commit()

    with container:
        st.success("Torrent added to seeds")


def download_from_torrent(file: FileModel, container):
    with container:
        _download_from_torrent(file)


def _download_from_torrent(file: FileModel):
    if not file.torrent:
        st.write("No torrent available")
        return

    try:
        db = connect_db()
        cursor = db.cursor()

        torrent_repo = TorrentsRepository(db, cursor)

        torrent_source = None
        try:
            data = aa_torrents.get_one(file.torrent)

            torrent_filename = os.path.basename(file.torrent)
            with open(torrent_filename, 'wb') as f:
                f.write(data)

            torrent_source = torrent_filename
        except Exception as e:
            print("Failed to download torrent file")

        if torrent_source is None:
            torrent_source = file.torrent_magnet_link

        if not torrent_source:
            raise Exception("No torrent source available")

        progress_bar = st.progress(0.0, text="Starting donwload")

        def on_progress(status: dict):
            progress = float(status.get('progress', 0.0))
            download_rate = float(status.get('download_rate', 0.0))

            progress_bar.progress(progress, text=f"Downloading: {download_rate / 1000}kb/s")

        server_path = file.server_path.split(';')[0]
        if not server_path:
            raise Exception("No filenames available")

        file_name = os.path.basename(server_path)
        downloader = TorrentDownloader()
        handle = downloader.download(torrent_source, file_name, progress_callback=on_progress)

        progress_bar.progress(1.0, text="Donwload complete")
        file_path = downloader.get_torrent_file_path_by_name(handle, file_name)

        if not file_path:
            st.error("Failed to get path for downloaded file")
            return

        full_path = os.path.join(DOWNLOADS_DIR, file_path)

        assert(file.torrent_id and file.file_id)
        torrent_repo.insert_file(TorrentFileModel(
            torrent_id=file.torrent_id,
            filename=file_name,
            file_id=file.file_id,
            is_complete=True,
            local_path=file_path,
        ))
        db.commit()
        st.cache_data.clear()

        with open(full_path, 'rb') as f:
            st.download_button("Get file", f, file_name=f"{file.md5}.{file.extension}", on_click='ignore')

    except Exception as e:
        st.error(e)


def scroll_to_top():
    html(
        """
        <script>
            window.parent.document.querySelector('section.stMain').scrollTo(0, 0)
        </script>
        """,
        height=0
    )

@st.fragment()
def extract_btn(file: FileModel):
    db = connect_db()
    cursor = db.cursor()
    extractor = ByteoffsetFileExtractor(db, cursor)
    with st.empty():
        if st.button("Extract file"):
            data = extractor.extract(file)
            if data:
                st.download_button(
                    "⬇️ Get file", data,
                    key=f"download_extract_{file.file_id}",
                    file_name=f"{file.md5}.{file.extension}",
                )
            else:
                st.error("Failed to extract data")


def format_file_result(file: FileModel):
    """Render file details in Streamlit-friendly format."""
    ipfs_urls = []
    if file.ipfs_cid:
        ipfs_urls = [f"{UI_IPFS_GATEWAY}/ipfs/{cid}" for cid in set(file.ipfs_cid.split(';'))]

    with st.container():
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader(file.title or "-Untitled-", anchor=False)

            with st.container(horizontal=True):
                st.badge(file.extension, color="grey")
                if file.is_complete is not None:
                    if file.is_complete:
                        st.badge("complete", color="green")
                    else:
                        st.badge("not complete", color="red")

                if file.is_journal:
                    st.badge("journal", color="orange")

                if file.byteoffset:
                    st.badge("byteoffset", color="grey")

            st.write(f"**Author:** {file.author or '-'}")
            st.write(f"**Year:** {file.year or '-'}")

            st.write(f"**MD5:** `{file.md5}`")
            st.write(f"**Server Path:** `{file.server_path}`")

            with st.container(horizontal=True):
                if file.torrent:
                    st.markdown(
                        f"**Torrent:** [Download](https://annas-archive.org/dyn/small_file/torrents/{file.torrent})",
                        width="content"
                    )


                if file.torrent_magnet_link:
                    st.markdown(
                        f"**Magnet:** [Open]({file.torrent_magnet_link})",
                        width="content"
                    )

                if file.torrent:
                    if st.button('🔗 Explore', key=f"view_torrent_{file.file_id}"):
                        st.session_state.torrent_id = file.torrent_id
                        st.session_state.torrent_name = file.torrent
                        st.session_state.reset_inputs = True
                        print(file.torrent_id)
                        st.rerun()

            for url in ipfs_urls:
                st.markdown(f"**IPFS:** [{url}]({url})")

            if file.description:
                wrapped = textwrap.fill(file.description, width=78)
                st.markdown(f"**Description:**\n\n{wrapped}")

            @st.fragment()
            def download_fr():
                container = st.container()
                with container:
                    st.button(
                        "⬇️ Download from torrent",
                        key=f"download_{file.md5}",
                        on_click=download_from_torrent,
                        args=(file, container,)
                    )

            if file.is_complete is None:
                download_fr()

            @st.fragment()
            def add_to_seeds():
                container = st.container()
                with container:
                    st.button(
                        "🌐 Add to seeds",
                        key=f"seed_{file.md5}",
                        on_click=seed_file,
                        args=(file, container,)
                    )

            if file.is_complete is None:
                add_to_seeds()
            else:
                with st.container(horizontal=True):
                    if file.is_complete and not file.byteoffset:
                        path = get_file_path(file)
                        if path is not None:
                            with open(path, 'rb') as f:
                                st.download_button(
                                    "⬇️ Get file", f.read(),
                                    key=f"download_{file.file_id}",
                                    file_name=f"{file.md5}.{file.extension}",
                                )
                    if file.is_complete and file.byteoffset:
                        extract_btn(file)

                    if st.button("❌ Remove seed", key=f"remove_{file.file_id}"):
                        svc.remove_from_seeds(file)
                        st.success("Seed removed")

        with col2:
            if file.cover_url:
                st.image(
                    file.cover_url,
                    width=200,
                )

def reset_pagination():
    st.session_state.offset = 0

def main():
    st.set_page_config(page_title="File Search Tool", page_icon="🔍", layout="wide")

    if "scroll_to_top" not in st.session_state:
        st.session_state.scroll_to_top = False

    if "torrent_id" not in st.session_state:
        st.session_state.torrent_id = None

    if "torrent_name" not in st.session_state:
        st.session_state.torrent_name = None

    if "reset_inputs" not in st.session_state:
        st.session_state.reset_inputs = None

    if st.session_state.reset_inputs:
        st.session_state.reset_inputs = False
        st.session_state.query_input = ''
        st.session_state.language_select = 'Any'
        st.session_state.year_input = ''

    if st.session_state.scroll_to_top:
        scroll_to_top()
        st.session_state.scroll_to_top = False

    # --- Sidebar filters ---
    with st.sidebar:
        st.header("Filters")
        language = st.selectbox(
            "Language",
            options=['Any', 'en', 'ru', 'zn'],
            index=0,
            key='language_select',
            on_change=reset_pagination,
        )
        year = st.text_input(
            "Year", placeholder="e.g. 2020 or leave blank",
            key='year_input',
            on_change=reset_pagination,
        )
        limit = st.selectbox(
            "Results per page", [10, 25, 50, 100],
            index=0,
            on_change=reset_pagination,
        )
        sort = st.selectbox(
            "Sort by",
            options=['relevance', 'none', 'year', 'title'],
            index=0,
        )
        sort_direction = st.selectbox(
            "Sort direction",
            options=['ascending', 'descending'],
            index=0,
        )

        local_only = st.checkbox(
            "Local only",
            on_change=reset_pagination,
        )

    # --- Main search input ---
    query = st.text_input(
        "Enter your search query:",
        placeholder="Type a keyword...",
        key="query_input",
        on_change=reset_pagination,
    )

    # Keep pagination state
    if "offset" not in st.session_state:
        st.session_state.offset = 0

    if st.session_state.torrent_id:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"Filtered by torrent `{st.session_state.torrent_name}`")

            @st.fragment()
            def add_torrent_to_seeds():
                container = st.container()
                with container:
                    st.button("Add to seeds", key=f"torrent_to_seeds", on_click=seed_torrent, args=(st.session_state.torrent_id, container,))

            add_torrent_to_seeds()

        with col2:
            if st.button("Clear"):
                st.session_state.torrent_id = None
                st.rerun()

    if st.button("🔍 Search", on_click=reset_pagination) or st.session_state.offset >= 0:
        search_lang = None if language == "Any" else language
        search_year = year or None

        search_torrent_id = st.session_state.torrent_id

        results = search(
            query,
            search_lang,
            search_year,
            search_torrent_id,
            limit,
            st.session_state.offset,
            sort,
            sort_direction,
            local_only=local_only,
        )

        st.text(f"Showing {len(results)} files ({st.session_state.offset} - {st.session_state.offset + len(results)})")

        if not results:
            st.warning("No results found.")
        else:
            for file in results:
                format_file_result(file)

            # Pagination controls
            col1, _, col3 = st.columns([1, 2, 1])
            with col1:
                if st.session_state.offset > 0:
                    if st.button("⬅️ Previous"):
                        st.session_state.offset = max(0, st.session_state.offset - limit)
                        st.session_state.scroll_to_top = True
                        st.rerun()
            with col3:
                if len(results) == limit:
                    if st.button("Next ➡️"):
                        st.session_state.offset += limit
                        st.session_state.scroll_to_top = True
                        st.rerun()
    else:
        st.info("Enter a search term above to get started.")


if __name__ == "__main__":
    main()
