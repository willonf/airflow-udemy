from helpers.services import BaseService

from helpers import urls


class GenreRequestData:

    def __init__(self):
        self.route = urls.GENRE
        self.service = BaseService(route=self.route)

    def _get_genres(self):
        return self.service.list()['data']['genres']

    def run(self):
        self._get_genres()


if __name__ == '__main__':
    artists = GenreRequestData()
    artists.run()
