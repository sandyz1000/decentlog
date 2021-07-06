import typing
import logging
import types
import importlib
from .subscriber import listen_channel_output, _SETTING_TYPE
from .dclogger import LogObject, DecentralizedLogger, StreamToLogger


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


def init_decentlog(settings: typing.Union[_SETTING_TYPE, typing.AnyStr] = None) -> logging.Logger:
    # - Return both log instances
    if isinstance(settings, str):
        module = importlib.import_module(settings)
        settings = _convert_to_dict(module)
    logger = DecentralizedLogger(settings).set_logger()
    return logger


__all__ = ['listen_channel_output', 'init_decentlog', 'StreamToLogger', 'LogObject', 'DecentralizedLogger']
