import streamlit as st

pg = st.navigation([
	st.Page("page_search.py", title="Search"),
	st.Page("page_seeds.py", title="Seeds")
])
pg.run()
