import glob
import streamlit as st
from sympy import true

from models.file import FileModel
from models.seed import SeedModel
from repositories.files import FilesRepository
from repositories.seeds import SeedsRepository
from utils.db import connect_db
from page_search import scroll_to_top

db = connect_db()
cursor = db.cursor()

seeds_repo = SeedsRepository(db, cursor)
files_repo = FilesRepository(db, cursor)

downloads_dir = "./downloads"

per_page = 10

def get_file_path(seed: SeedModel):
    if seed.path:
        return f"{downloads_dir}/{seed.path}"

    paths = glob.glob(f"{downloads_dir}/**/{seed.filename}", recursive=True)
    if len(paths):
        return paths[0]

    return None

def format_file_result(seed: SeedModel, file: FileModel):
    """Render file details in Streamlit-friendly format."""
    ipfs_urls = []
    if file.ipfs_cid:
        ipfs_urls = [f"https://ipfs.io/ipfs/{cid}" for cid in set(file.ipfs_cid.split(';'))]

    with st.container():
        st.markdown("---")
        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader(file.title or "Untitled")
            with st.container(horizontal=True):
                st.badge(file.extension, color="gray")

                if seed.is_complete:
                    st.badge("complete", color="green")
                else:
                    st.badge("not complete", color="red")

            st.write(f"**Author:** {file.author or '-'}")
            st.write(f"**Year:** {file.year or '-'}")

            st.write(f"**MD5:** `{file.md5}`")
            st.write(f"**Server Path:** `{file.server_path}`")

            with st.expander("Links"):
                if file.torrent:
                    st.markdown(f"**Torrent:** [Download](https://annas-archive.org/dyn/small_file/torrents/{file.torrent})")

                if file.torrent_magnet_link:
                    st.markdown(f"**Magnet:** [Open]({file.torrent_magnet_link})")

                for url in ipfs_urls:
                    st.markdown(f"**IPFS:** [{url}]({url})")

            if file.description:
                st.markdown(f"**Description:**\n\n{file.description}")

            with st.container(horizontal=True):
                if seed.is_complete:
                    path = get_file_path(seed)
                    if path is not None:
                        with open(path, 'rb') as f:
                            st.download_button(
                                "⬇️ Get file", f.read(),
                                key=f"download_{seed.seed_id}",
                                file_name=f"{file.md5}.{file.extension}",
                        )
                if st.button("❌ Remove seed", key=f"remove_{seed.seed_id}"):
                    assert(seed.seed_id is not None)
                    seeds_repo.remove(seed.seed_id)
                    db.commit()
                    st.success("Seed removed")




        with col2:
            if file.cover_url:
                st.image(file.cover_url, width=200)

def main():
    st.set_page_config(page_title="Seeds", page_icon="🔍", layout="wide")

    if "seeds_page" not in st.session_state:
        st.session_state.seeds_page = 0

    if "scroll_to_top" not in st.session_state:
        st.session_state.scroll_to_top = False

    if st.session_state.scroll_to_top:
        scroll_to_top()
        st.session_state.scroll_to_top = False

    page = st.session_state.seeds_page
    seeds = seeds_repo.list(limit=per_page, offset=page * per_page)
    file_ids = [seed.file_id for seed in seeds]
    files = {file.file_id: file for file in files_repo.find_by_ids(file_ids)}

    st.header("Seeds")
    for seed in seeds:
        format_file_result(seed, files[seed.file_id])

    with st.container(horizontal=True):
        if st.button(f"Prev {st.session_state.seeds_page}", disabled=st.session_state.seeds_page == 0):
            st.session_state.seeds_page = max(0, st.session_state.seeds_page - 1)
            st.session_state.scroll_to_top = True
            st.rerun()

        st.space("stretch")

        if st.button(f"Next {st.session_state.seeds_page + 1}"):
            st.session_state.seeds_page += 1
            st.session_state.scroll_to_top = True
            st.rerun()




if __name__ == '__main__':
    main()
