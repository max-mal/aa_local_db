import glob
import streamlit as st

from models.file import FileModel
from models.seed import SeedModel
from repositories.files import FilesRepository
from repositories.seeds import SeedsRepository
from utils.db import connect_db

db = connect_db()
cursor = db.cursor()

seeds_repo = SeedsRepository(db, cursor)
files_repo = FilesRepository(db, cursor)

downloads_dir = "./downloads"

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

            if seed.is_complete:
                st.badge("complete", color="green")
            else:
                st.badge("not complete", color="red")

            st.write(f"**Author:** {file.author or '-'}")
            st.write(f"**Year:** {file.year or '-'}")

            st.write(f"**MD5:** `{file.md5}`")
            st.write(f"**Server Path:** `{file.server_path}`")

            if file.torrent:
                st.markdown(f"**Torrent:** [Download](https://annas-archive.org/dyn/small_file/torrents/{file.torrent})")

            if file.torrent_magnet_link:
                st.markdown(f"**Magnet:** [Open]({file.torrent_magnet_link})")

            for url in ipfs_urls:
                st.markdown(f"**IPFS:** [{url}]({url})")

            if file.description:
                st.markdown(f"**Description:**\n\n{file.description}")

            if st.button("Remove seed", key=f"remove_{seed.seed_id}"):
                assert(seed.seed_id is not None)
                seeds_repo.remove(seed.seed_id)
                db.commit()
                st.success("Seed removed")


            if seed.is_complete:
                path = get_file_path(seed)
                if path is not None:
                    with open(path, 'rb') as f:
                        st.download_button(
                            "Get file", f.read(),
                            key=f"download_{seed.seed_id}",
                            file_name=f"{file.md5}.{file.extension}",
                    )

        with col2:
            if file.cover_url:
                st.image(file.cover_url, width=200)

def main():
    seeds = seeds_repo.list()
    file_ids = [seed.file_id for seed in seeds]
    files = {file.file_id: file for file in files_repo.find_by_ids(file_ids)}

    st.header("Seeds")
    for seed in seeds:
        format_file_result(seed, files[seed.file_id])


if __name__ == '__main__':
    main()
