import helpers.utils
from helpers import services, urls
import os


class ArtistRequestData:

    def __init__(self):
        self.route = urls.ARTIST
        self.service = services.BaseService(route=self.route)

    def _get_artists(self):
        with open('artist-ids.csv', 'r') as file:
            lines = file.readlines()
            print(self.service.get_by_id(pk=lines[0].replace('\n', '')))

    def run(self):
        self._get_artists()


if __name__ == '__main__':
    helpers.utils.load_environment_variables()

    artists = ArtistRequestData()
    artists.run()
