import streamlit as st
import pandas as pd

from repositories.torrents import TorrentsRepository
from services.torrent import TorrentService
from utils.db import connect_db
from seeder_server import get_status


db = connect_db()
cursor = db.cursor()


torerents_repo = TorrentsRepository(db, cursor)
torrents_svc = TorrentService(db, cursor)

@st.cache_data(ttl=5)
def get_torrents_data():
    torrents_data = {
        "Torrent": [],
        "Seeding": [],
        "Date": [],
        "Size": [],
        "Num Files": [],
        "Seeding Files": [],
        "Obsolete": [],
        "Embargo": [],
        "magnet_link": [],
        "id": [],
        "progress": [],
        "download_rate": [],
        "upload_rate": [],
    }

    try:
        torrents_status = get_status()
    except Exception as e:
        print(e)
        torrents_status = {}
        

    torrents = torerents_repo.list()
    torrents_svc.populate_files(torrents)

    for t in torrents:
        torrents_data['Torrent'].append(t.path)
        torrents_data['Date'].append(t.added_to_torrents_list_at)
        torrents_data['Num Files'].append(t.num_files)

        torrents_data['Obsolete'].append("🔴 Yes" if t.obsolete else "🟢 No")
        torrents_data['Embargo'].append("🔴 Yes" if t.embargo else "🟢 No")
        torrents_data['Seeding'].append("🟢 Yes" if t.is_seeding else "🔴 No")
        torrents_data['id'].append(t.torrent_id)
        torrents_data['magnet_link'].append(t.magnet_link)

        if t.torrent_id in torrents_status:
            status = torrents_status[t.torrent_id]
            torrents_data['progress'].append(status.progress)
            torrents_data['download_rate'].append(status.download_rate)
            torrents_data['upload_rate'].append(status.upload_rate)
        else:
            torrents_data['progress'].append(None)
            torrents_data['download_rate'].append(None)
            torrents_data['upload_rate'].append(None)

        if t.data_size:
            torrents_data['Size'].append("{:10.2f}".format(t.data_size / 1024 / 1024 / 1024) + 'GB')
        else:
            torrents_data['Size'].append(None)

        if t.is_seeding and not len(t.files):
            torrents_data['Seeding Files'].append('All')
        else:
            torrents_data['Seeding Files'].append(len(t.files))

    df = pd.DataFrame(torrents_data)
    return df

def main():
    st.set_page_config(page_title="Torrents", page_icon="🔍", layout="wide")
    df = get_torrents_data()

    container = st.container()
    status = st.empty()

    if "success" not in st.session_state:
        st.session_state.success = None


    if st.session_state.success:
        status.success(st.session_state.success)
        st.session_state.success = None

    event = st.dataframe(
        df,
        height='auto',
        selection_mode="single-row",
        on_select="rerun"
    )

    selections = event.get('selection', {}).get('rows', [])
    if len(selections):
        selected_row = selections[0]
        row = df.iloc[selected_row]
        st.session_state.selected_torrent = row

        with container:
            st.markdown(f"Torrent: `{row['Torrent']}`")

        with container.container(horizontal=True):
            torrent_id = int(row['id'])
            if st.button("Seed"):
                torrents_svc.seed_torrent(torrent_id, seed_all=True)
                st.session_state.success = f"Torrent added"
                st.cache_data.clear()
                st.rerun()

            if not pd.isna(row['Seeding Files']):
                if st.button("Stop seeding"):
                    torrents_svc.stop_seed_torrent(torrent_id)
                    st.session_state.success = f"Torrent removed"
                    st.cache_data.clear()
                    st.rerun()

            st.link_button("Explore", url=f'/?torrent_id={torrent_id}')


if __name__ == '__main__':
    main()
