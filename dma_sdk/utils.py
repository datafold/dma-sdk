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
    try:
        res = requests.post(url, files=files, data=data, json=json_data, headers=headers)
        check_requests_result(res)
        return res
    except requests.exceptions.ConnectionError as exception:
        parsed_uri = urlparse(url)
        print(f"The host {parsed_uri.netloc} could not be reached")
        raise exception


def get_data(url, headers, params=None):
    try:
        res = requests.get(url, headers=headers, params=params or {})
        check_requests_result(res)
        return res
    except requests.exceptions.ConnectionError as exception:
        parsed_uri = urlparse(url)
        print(f"The host {parsed_uri.netloc} could not be reached")
        raise exception


def check_requests_result(res):
    try:
        res.raise_for_status()
    except requests.HTTPError:
        print('Error: %s', res.text)
        raise
