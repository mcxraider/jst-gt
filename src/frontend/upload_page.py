import streamlit as st


from frontend.components.page_header.page_header import *
from frontend.components.upload_page.file_selector import file_selector
from frontend.components.upload_page.sector_selector import sector_selector


def upload_file_page():
    create_header()
    sector_selector()
    file_selector()
