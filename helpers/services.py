import requests
from requests.exceptions import RequestException
import os
import base64

from helpers import utils

TOKEN = None


class BaseService:

    def __init__(self, route=''):
        self.request = requests.sessions.Session()
        self.headers = {}
        self.token_url = os.environ['API_TOKEN_URL']
        self.base_url = os.environ['API_URL_BASE']
        self.url = self._build_url(route)
        self.__client_id = os.environ['CLIENT_ID']
        self.__client_secret = os.environ['CLIENT_SECRET']
        self._make_headers()

    def _make_headers(self):
        try:
            self._get_token()
            if TOKEN:
                self.headers['Content-Type'] = 'application/json'
                self.headers['Authorization'] = f'Bearer {TOKEN}'
        except Exception as ex:
            raise ex

    def _build_url(self, route):
        return f'{self.base_url}/{route}'

    def _get_token(self):
        global TOKEN

        try:
            if not TOKEN:
                auth_bytes = f"{self.__client_id}:{self.__client_secret}".encode(
                    'utf-8')
                auth_base64 = str(base64.b64encode(auth_bytes), 'utf-8')

                request = self.request.post(
                    url=self.token_url,
                    headers={
                        "Authorization": f"Basic {auth_base64}",
                        "Content-Type": "application/x-www-form-urlencoded"},
                    data={"grant_type": "client_credentials"}
                )
                response = utils.get_response(request)
                TOKEN = response['data']['access_token']
        except RequestException as ex:
            raise ex

    def list(self, params=None):
        try:
            request = self.request.get(
                headers=self.headers,
                url=self.url,
                params=params
            )
            return utils.get_response(request)
        except RequestException as ex:
            raise ex

    def get_by_id(self, pk, params=None):
        try:
            request = self.request.get(
                headers=self.headers,
                url=f'{self.url}/{pk}',
                params=params
            )
            return utils.get_response(request)
        except RequestException as ex:
            raise ex
