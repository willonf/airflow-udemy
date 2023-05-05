import os
from os.path import exists
from dotenv import load_dotenv

from landing.artists.artists import ArtistRequestData

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CONF_PATH = os.path.join(BASE_DIR, '.conf')

if exists(CONF_PATH):
    load_dotenv(CONF_PATH)

artists = ArtistRequestData()
artists.run()
