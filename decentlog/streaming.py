import datetime
import json
import logging
import http.client
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# base url for all requests
BASE_URL = 'dweet.io'


def _check_stream_timeout(started: int, timeout: int):
    """Check if the timeout has been reached and raise a `StopIteration` if so.
    """
    if timeout:
        elapsed = datetime.datetime.utcnow() - started
        if elapsed.seconds > timeout:
            raise StopIteration


def _listen_for_dweets_from_response(response: http.client.HTTPResponse):
    """Yields dweets as received from dweet.io's streaming API
    """
    streambuffer = ''
    while not response.closed:
        byte = response.read1()
        if byte:
            streambuffer += byte.decode('ascii')
            try:
                dweet = json.loads(streambuffer.splitlines()[1])
            except (IndexError, ValueError):
                continue
            if isinstance(dweet, str):
                yield json.loads(dweet)
            streambuffer = ''


def listen_for_dweets_from(thing_name: str, timeout: int = 900, reconnect: bool = True):
    headers = {'Content-type': 'application/json', 'Connection': 'keep-alive'}
    start = datetime.datetime.utcnow()
    uri = "/listen/for/dweets/from/{0}".format(thing_name)
    while True:
        conn = http.client.HTTPSConnection(BASE_URL, timeout=timeout)
        conn.request("GET", uri, headers=headers, encode_chunked=True)
        resp = conn.getresponse()
        try:
            for x in _listen_for_dweets_from_response(resp):
                yield x
                _check_stream_timeout(start, timeout)
        except (http.client.error) as e:
            if reconnect:
                logger.error("Connection timeout, reconnecting ....")
                start = datetime.datetime.utcnow()
                continue
            raise e
