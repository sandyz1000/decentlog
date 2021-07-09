import logging
import typing
import dweepy
import requests
import datetime
import json
import http.client
from .utils import polling
from kafka.consumer import KafkaConsumer
from abc import ABCMeta, abstractmethod

logger = logging.getLogger("__main__")
_SETTING_TYPE = typing.Dict[typing.AnyStr, typing.Any]


class BaseListener(ABCMeta):
    """ Base Publisher """

    def __init__(self, settings: _SETTING_TYPE):
        self.settings = settings

    def _check_stream_timeout(self, started: int, timeout: int):
        """Check if the timeout has been reached and raise a `StopIteration` if so."""
        if timeout:
            elapsed = datetime.datetime.utcnow() - started
            if elapsed.seconds > timeout:
                raise StopIteration

    @abstractmethod
    def listen_channel_output(self) -> typing.Iterator:
        """ This will be call by client to listen for new message appear in the channel """
        ...


class KafkaListener(BaseListener):
    def __init__(self, settings: _SETTING_TYPE):
        super(KafkaListener, self).__init__(settings)
        if 'KAFKA' not in settings:
            raise AttributeError("Key with named 'KAFKA' not found")
        self.settings = settings
        kafka_cfg = self.settings.get('KAFKA')
        self.consumer = KafkaConsumer(
            kafka_cfg['HOSTS'],
            group_id=kafka_cfg['GROUP_ID'],
            request_timeout_ms=kafka_cfg.get('TIMEOUT', 200000),
            bootstrap_servers=kafka_cfg.get('BOOTSTRAP_SERVER', 'localhost:9092'),
            value_deserializer=lambda m: json.loads(m.decode('ascii')),
        )

    def listen_channel_output(self) -> typing.Iterator:
        """ This will be call by client to listen for new message appear in the channel """
        try:
            for message in self.consumer:
                _, _, _, key, msg = (message.topic, message.partition,
                                     message.offset, message.key, message.value)
                yield msg
        except Exception as e:
            raise e


class DweetHttpListener(BaseListener):
    def __init__(self, settings: _SETTING_TYPE):
        super(DweetHttpListener, self).__init__(settings)
        if 'DWEET' not in settings:
            raise AttributeError("Key with named 'DWEET' not found")
        dweet_cfg = settings.get('DWEEET')
        self.BASE_URL = dweet_cfg['BASE_URL']
        self.conn = http.client.HTTPSConnection(self.BASE_URL, timeout=dweet_cfg['timeout'])
        self.channel = dweet_cfg['THINGS_NAME']
        self.uri = "/listen/for/dweets/from/{0}".format(self.channel)
        self.headers = {"Content-type": "application/json", "Connection": "keep-alive"}
        self.timeout = dweet_cfg['TIMEOUT']

    def parse_http_response(self, response: http.client.HTTPResponse) -> typing.Iterator:
        """Yields dweets as received from dweet.io's streaming API"""
        streambuffer = ""
        while not response.closed:
            byte = response.read1()
            if not byte:
                continue
            streambuffer += byte.decode("ascii")
            try:
                dweet = json.loads(streambuffer.splitlines()[1])
            except (IndexError, ValueError):
                continue
            if isinstance(dweet, str):
                yield json.loads(dweet)
            streambuffer = ""

    def poll_dweet_things_from(self) -> typing.Iterator:
        """ Poll dweet API to look for message
        """
        _delay = 1
        session = requests.Session()
        oldhash = polling._generate_hash({})
        collect_values = polling.Queue(-1)
        while True:
            try:
                for dweet in polling.poll(
                    dweepy.get_latest_dweet_for,
                    step=_delay,
                    args=(self.channel,),
                    kwargs={"session": session},
                    check_success=lambda new: oldhash != polling._generate_hash(new[0]),
                    timeout=self.timeout,
                    collect_values=collect_values,
                    ignore_exceptions=(
                        requests.exceptions.ChunkedEncodingError,
                        requests.ReadTimeout,
                        requests.ConnectionError,
                        dweepy.DweepyError,
                    ),
                    # poll_forever=True
                ):
                    oldhash = polling._generate_hash(dweet)
                    yield dweet
            except (polling.PollingException, ) as e:
                logging.error("Exception occur while polling: ", str(e))
                raise e

    def listen_for_dweets_from(self, reconnect: bool = True):
        start = datetime.datetime.utcnow()
        while True:
            self.conn.request("GET", self.uri, headers=self.headers, encode_chunked=True)
            resp = self.conn.getresponse()
            try:
                for x in self.parse_http_response(resp):
                    yield x
                    self._check_stream_timeout(start, self.timeout)
            except (http.client.error) as e:
                if reconnect:
                    logger.error("Connection timeout, reconnecting ....")
                    start = datetime.datetime.utcnow()
                    self.conn = http.client.HTTPSConnection(self.BASE_URL, timeout=self.timeout)
            except Exception as e:
                raise e

    def listen_channel_output(self, longpoll: bool = False) -> typing.Iterator:
        """Listen to log publish in the dweet channel from remote server

        :raises Exception: [description]
        :return: [description]
        :rtype: [type]
        """
        func = self.listen_for_dweets_from if not longpoll else self.poll_dweet_things_from
        try:
            for dweet in func():
                msg, _, _ = (
                    dweet["content"].get("msg", None),
                    dweet["created"],
                    dweet["thing"],
                )
                yield msg
        except Exception as e:
            raise e
