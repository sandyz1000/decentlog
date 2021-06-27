import logging
import typing
import dweepy
import requests
import datetime
import json
import http.client
from .utils import polling

logger = logging.getLogger('__main__')

# base url for all requests
BASE_URL = 'dweet.io'
_session_type = typing.Union[requests.Session, typing.Any]


def _check_stream_timeout(started: int, timeout: int):
    """Check if the timeout has been reached and raise a `StopIteration` if so.
    """
    if timeout:
        elapsed = datetime.datetime.utcnow() - started
        if elapsed.seconds > timeout:
            raise StopIteration


def _listen_for_dweets_from_response(response: http.client.HTTPResponse) -> None:
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


def poll_dweet_things_from(channel: str, timeout: int = 900,
                           session: _session_type = None, 
                           **kwargs) -> typing.Generator:
    """
    Poll dweet API to look for message
    :param channel: Things/Channel name
    :type channel: str
    :param timeout: [description], defaults to 2000
    :type timeout: [type], optional
    """
    _delay = 1
    session = session or requests.Session()
    oldhash = polling._generate_hash({})
    collect_values = polling.Queue(-1)
    while True:
        try:
            for dweet in polling.poll(
                dweepy.get_latest_dweet_for,
                step=_delay,
                args=(channel, ), kwargs={"session": session},
                check_success=lambda new: oldhash != polling._generate_hash(new[0]),
                timeout=timeout,
                collect_values=collect_values,
                ignore_exceptions=(requests.exceptions.ChunkedEncodingError, requests.ReadTimeout,
                                   requests.ConnectionError, dweepy.DweepyError),
                # poll_forever=True
            ):
                oldhash = polling._generate_hash(dweet)
                yield dweet
        except (polling.PollingException, ) as e:
            logging.error("Exception occur while polling: ", str(e))
            raise e


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


def listen_channel_output(channel, timeout=200000, longpoll=False, **kwargs):
    """ Listen to log publish in the dweet channel from remote server

    :raises Exception: [description]
    :return: [description]
    :rtype: [type]
    """
    func_dweet = poll_dweet_things_from if longpoll else listen_for_dweets_from
    try:
        for dweet in func_dweet(channel, timeout=timeout):
            msg, _, _ = dweet['content'].get('msg', None), dweet['created'], dweet['thing']
            yield msg
    except Exception as e:
        raise e

