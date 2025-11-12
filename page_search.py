import streamlit as st
import textwrap
from models.file import FileModel
from models.seed import SeedModel
from repositories.aa_torrents import AnnasArchiveTorrentsRepository
from repositories.files import FilesRepository
from repositories.seeds import SeedsRepository
from services.files import FilesService
from utils.db import connect_db, interrupt_after
from utils.torrent import TorrentDownloader
import os
import glob

from streamlit.components.v1 import html

# Initialize DB and service
db = connect_db()
cursor = db.cursor()
svc = FilesService(db, cursor)
repo = FilesRepository(db, cursor)

interrupt_after(15, db)

aa_torrents = AnnasArchiveTorrentsRepository()


@st.cache_data
def search(
    query,
    search_lang,
    search_year,
    search_torrent_id,
    limit,
    offset,
    sort,
    sort_direction
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

    return repo.search(
        query_text=query,
        language=search_lang,
        year=search_year,
        torrent_id=search_torrent_id,
        limit=limit,
        offset=offset,
        order_by=order_by,
    )

def seed_file(file: FileModel, container):
    db = connect_db()
    cursor = db.cursor()

    svc = FilesService(db, cursor)
    svc.add_to_seeds(file)

    db.commit()

    with container:
        st.success("File added to seeds")

def seed_torrent(torrent_id, container):
    db = connect_db()
    cursor = db.cursor()

    files_repo = FilesRepository(db, cursor)
    svc = FilesService(db, cursor)

    with container:
        status = st.progress(0, text="Adding torrent")

        offset = 0
        while True:
            files = files_repo.search(torrent_id=torrent_id, limit=100, offset=offset)
            if not len(files):
                break

            for file in files:
                svc.add_to_seeds(file)

            db.commit()
            offset += 100
            status.progress(0, text=f"Adding files: {offset}")


        status.empty()
        st.success("Torrent added to seeds")


def download_from_torrent(file: FileModel, container):
    with container:
        _download_from_torrent(file)


def _download_from_torrent(file: FileModel):
    if not file.torrent:
        st.write("No torrent available")
        return

    try:
        if not file.torrent_magnet_link:
            data = aa_torrents.get_one(file.torrent)

            torrent_filename = os.path.basename(file.torrent)
            with open(torrent_filename, 'wb') as f:
                f.write(data)

            torrent_source = torrent_filename
        else:
            torrent_source = file.torrent_magnet_link

        progress_bar = st.progress(0.0, text="Starting donwload")

        def on_progress(status: dict):
            progress = float(status.get('progress', 0.0))
            download_rate = float(status.get('download_rate', 0.0))

            progress_bar.progress(progress, text=f"Downloading: {download_rate / 1000}kb/s")

        file_name = os.path.basename(file.server_path)
        downloader = TorrentDownloader()
        downloader.download(torrent_source, file_name, progress_callback=on_progress)

        progress_bar.progress(1.0, text="Donwload complete")

        file_paths = glob.glob(f'downloads/**/{file_name}')
        if not len(file_paths):
            st.error("Failed to get path for downloaded file")
            return

        with open(file_paths[0], 'rb') as f:
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


def format_file_result(file: FileModel):
    """Render file details in Streamlit-friendly format."""
    ipfs_urls = []
    if file.ipfs_cid:
        ipfs_urls = [f"https://ipfs.io/ipfs/{cid}" for cid in set(file.ipfs_cid.split(';'))]

    with st.container():
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader(file.title or "-Untitled-", anchor=False)
            st.badge(file.extension, color="grey")

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

            add_to_seeds()

        with col2:
            if file.cover_url:
                st.image(
                    file.cover_url,
                    width=200,
                )


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
            key='language_select'
        )
        year = st.text_input(
            "Year", placeholder="e.g. 2020 or leave blank",
            key='year_input'
        )
        limit = st.selectbox("Results per page", [10, 25, 50, 100], index=0)
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

    # --- Main search input ---
    query = st.text_input(
        "Enter your search query:",
        placeholder="Type a keyword...",
        key="query_input"
    )

    # Keep pagination state
    if "offset" not in st.session_state:
        st.session_state.offset = 0

    def reset_pagination():
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
            sort_direction
        )

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
