import json
import os
from os.path import exists

from dotenv import load_dotenv


def get_response(request):
    result_response = {'ok': request.ok, 'status_code': request.status_code}

    if request.ok:
        try:
            result_response['data'] = request.json()
        except ValueError:
            pass
    else:
        try:
            result_response['data'] = json.loads(request.content)
        except Exception:
            result_response['data'] = None

    return result_response


def load_environment_variables():
    base_dir = os.path.dirname(os.path.abspath('./'))

    conf_path = os.path.join(base_dir, '.conf')

    if exists(conf_path):
        load_dotenv(conf_path)
