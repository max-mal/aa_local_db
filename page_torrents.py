import streamlit as st
import pandas as pd

from repositories.seeds import SeedsRepository
from repositories.torrents import TorrentsRepository
from utils.db import connect_db

db = connect_db()
cursor = db.cursor()


torerents_repo = TorrentsRepository(db, cursor)
seeds_repo = SeedsRepository(db, cursor)

@st.cache_data
def get_torrents_data():
	torrents_data = {
		"Torrent": [],
		"Date": [],
		"Size": [],
		"Num Files": [],
		"Seeding Files": [],
		"Obsolete": [],
		"Embargo": [],
	}

	torrents = torerents_repo.list()
	seeds = {
		item['magnet_link']: item['count']
		for item in seeds_repo.count_by_magnets()
	}

	for t in torrents:
		torrents_data['Torrent'].append(t.path)
		torrents_data['Date'].append(t.added_to_torrents_list_at)
		torrents_data['Num Files'].append(t.num_files)
		torrents_data['Seeding Files'].append(seeds.get(t.magnet_link))
		torrents_data['Obsolete'].append("🔴 Yes" if t.obsolete else "🟢 No")
		torrents_data['Embargo'].append("🔴 Yes" if t.embargo else "🟢 No")

		if t.data_size:
			torrents_data['Size'].append("{:10.2f}".format(t.data_size / 1024 / 1024 / 1024) + 'GB')
		else:
			torrents_data['Size'].append(None)

	df = pd.DataFrame(torrents_data)
	return df

def main():
	st.set_page_config(page_title="Torrents", page_icon="🔍", layout="wide")
	df = get_torrents_data()
	st.dataframe(df, height=600)


if __name__ == '__main__':
	main()
