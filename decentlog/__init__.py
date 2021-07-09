import os
import sys
import typing
import logging
import types
import importlib
from .subscriber import _SETTING_TYPE, KafkaListener, DweetHttpListener
from .publisher import KafkaPublisher, DweetPublisher, StreamPublisher
from .dclogger import (LogObject, BaseDecentLogger, StreamToLogger,
                       FixedConcurrentRotatingFileHandler, ConcurrentRotatingFileHandler)


def _convert_to_dict(settings: types.ModuleType):
    '''
    Converts a settings file into a dictionary, ignoring python defaults
    @param setting: A loaded setting module
    '''
    ignore = [
        '__builtins__',
        '__file__',
        '__package__',
        '__doc__',
        '__name__',
    ]

    return {key: getattr(settings, key) for key in dir(settings) if key not in ignore}


class DecentralizedLogger(BaseDecentLogger):

    def resolve_handler(self):
        if self.settings.LOG_HANDLER.get('to_dweet', False):
            from .publisher import DweetPublisher
            self.logger.set_handler(DweetPublisher(self.settings))

        if self.settings.LOG_HANDLER.get('to_kafka', False):
            from .publisher import KafkaPublisher
            self.logger.set_handler(KafkaPublisher(self.settings))

        if self.settings.LOG_HANDLER.get('to_stream', False):
            from .publisher import StreamPublisher
            log_level = self.settings['LOG_LEVEL']
            self.logger.set_handler(StreamPublisher(log_level=log_level, dc_stdout=sys.stdout, dc_stderr=sys.stderr))

        if self.settings.LOG_HANDLER.get('to_file', False):
            my_bytes = self.settings['LOG_FORMATTER'].get('log_max_bytes', '10MB')
            my_file = "%s.log" % self.name
            my_backups = self.settings['LOG_FORMATTER'].get('log_backups', 5)

            if os.name == "nt":
                handler = FixedConcurrentRotatingFileHandler
            else:
                handler = ConcurrentRotatingFileHandler
            handler(os.path.join(self.my_dir, my_file), backupCount=my_backups, maxBytes=my_bytes)


def init_decentlog(settings: typing.Union[_SETTING_TYPE, typing.AnyStr] = None) -> logging.Logger:
    # - Return both log instances
    if settings is None:
        settings = os.path.join(os.path.dirname(__file__), "default_settings.py")
    if isinstance(settings, str):
        module = importlib.import_module(settings)
        settings = _convert_to_dict(module)
    logger = DecentralizedLogger(settings).set_logger()
    return logger


__all__ = [
    'listen_channel_output', 'init_decentlog',
    'StreamToLogger', 'KafkaListener',
    'DweetHttpListener', 'LogObject',
    'DecentralizedLogger', 'KafkaPublisher',
    'DweetPublisher', 'StreamPublisher',
]
