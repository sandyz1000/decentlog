import datetime
import json
import dweepy
import requests
import hashlib
import typing
import time
from requests.exceptions import ChunkedEncodingError


# base url for all requests
BASE_URL = 'https://dweet.io'


def _check_stream_timeout(started, timeout):
    """Check if the timeout has been reached and raise a `StopIteration` if so.
    """
    if timeout:
        elapsed = datetime.datetime.utcnow() - started
        if elapsed.seconds > timeout:
            raise StopIteration


def _listen_for_dweets_from_response(response):
    """Yields dweets as received from dweet.io's streaming API
    """
    streambuffer = ''
    for byte in response.iter_content():
        if byte:
            streambuffer += byte.decode('ascii')
            try:
                dweet = json.loads(streambuffer.splitlines()[1])
            except (IndexError, ValueError):
                continue
            if isinstance(dweet, str):
                yield json.loads(dweet)
            streambuffer = ''


def _generate_hash(dictionary: typing.Dict[str, typing.Any]) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.md5()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def listen_for_dweets_from(thing_name, timeout=900, key=None, session=None):
    """Create a real-time subscription to dweets
    """
    url = BASE_URL + '/listen/for/dweets/from/{0}'.format(thing_name)
    session = session or requests.Session()
    if key is not None:
        params = {'key': key}
    else:
        params = None

    start = datetime.datetime.utcnow()
    while True:
        request = requests.Request("GET", url, params=params).prepare()
        resp = session.send(request, stream=True, timeout=timeout)
        try:
            for x in _listen_for_dweets_from_response(resp):
                yield x
                _check_stream_timeout(start, timeout)
        except (ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            pass
        _check_stream_timeout(start, timeout)
        start = datetime.datetime.now()


def poll_dweet_things_from(channel, timeout=900, **kwargs):
    """
    Poll dweet API to look for message
    :param channel: Things/Channel name
    :type channel: str
    :param timeout: [description], defaults to 2000
    :type timeout: [type], optional
    """
    start = datetime.datetime.utcnow()
    oldhash = _generate_hash({})
    while True:
        try:
            for dweet in dweepy.get_latest_dweet_for(channel):
                if _generate_hash(dweet) != oldhash:    
                    oldhash = _generate_hash(dweet)
                    yield dweet
                    _check_stream_timeout(start, timeout)
                time.sleep(1)
        except (ChunkedEncodingError, requests.ReadTimeout, requests.ConnectionError):
            pass
        _check_stream_timeout(start, timeout)
        start = datetime.datetime.utcnow()
