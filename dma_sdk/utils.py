from urllib.parse import urlparse
import requests


def prepare_api_url(host: str, api_segment: str):
    return f"{host.rstrip('/')}/{api_segment}"


def prepare_headers(api_key: str):
    headers = {
        "Authorization": f"Key {api_key}",
    }
    return headers


def post_data(url, data=None, json_data=None, headers=None, files=None):
    res = requests.post(url, files=files, data=data, json=json_data, headers=headers)
    res.raise_for_status()
    return res


def get_data(url, headers, params=None):
    res = requests.get(url, headers=headers, params=params or {})
    res.raise_for_status()
    return res
