import sys
import logging
from .subscriber import listen_channel_output
from .dclogger import LogObject, DecentralizedLogger, StreamToLogger, SettingsConfigurator


def init_decentlog(settings: SettingsConfigurator = None) -> logging.Logger:
    # - Return both log instances
    logger = DecentralizedLogger(settings).set_logger()
    return logger


__all__ = ['listen_channel_output', 'init_decentlog', 'StreamToLogger', 'LogObject', 'DecentralizedLogger']
