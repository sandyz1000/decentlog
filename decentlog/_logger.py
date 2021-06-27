import copy
import datetime
import errno
import logging
import os
import sys
import typing
from functools import wraps
from portalocker import lock, unlock, LOCK_EX
from concurrent_log_handler import ConcurrentRotatingFileHandler, NullLogRecord

from pythonjsonlogger import jsonlogger
from collections import defaultdict
from . import default_settings
import importlib
import imp

_KT = typing.TypeVar("_KT")
_VT = typing.TypeVar("_VT")


class DecentLogError(Exception):
    pass


class SettingsWrapper(defaultdict):
    '''
    Wrapper for loading settings files and merging them with overrides
    '''

    my_settings = {}
    ignore = [
        '__builtins__',
        '__file__',
        '__package__',
        '__doc__',
        '__name__',
    ]

    def _init__(self):
        pass

    def __len__(self) -> int:
        return len(self.my_settings)

    def __getitem__(self, k: _KT) -> _VT:
        return self.my_settings[k]

    def __setitem__(self, k: _KT, v: _VT) -> None:
        self.my_settings[k] = v

    def __delitem__(self, v: _KT) -> None:
        del self.my_settings[v]

    def __iter__(self) -> typing.Iterator[_KT]:
        for k, _ in self.my_settings.items():
            yield k

    def load(self, local: str = 'localsettings.py', default: str = 'settings.py'):
        '''
        Load the settings dict

        @param local: The local settings filename to use
        @param default: The default settings module to read
        @return: A dict of the loaded settings
        '''
        self._load_defaults(default)
        self._load_custom(local)

        return self.settings()

    def load_from_string(self, settings_string='', module_name='customsettings'):
        '''
        Loads settings from a settings_string. Expects an escaped string like
        the following:
            "NAME=\'stuff\'\nTYPE=[\'item\']\n"

        @param settings_string: The string with your settings
        @return: A dict of loaded settings
        '''
        try:
            mod = imp.new_module(module_name)
            exec(settings_string in mod.__dict__)
        except TypeError:
            print("Could not import settings")
        self.my_settings = {}
        try:
            self.my_settings = self._convert_to_dict(mod)
        except ImportError:
            print("Settings unable to be loaded")

        return self.settings()

    def settings(self):
        '''
        Returns the current settings dictionary
        '''
        return self.my_settings

    def _load_defaults(self, default='settings.py'):
        '''
        Load the default settings
        '''
        if isinstance(default, str) and default[-3:] == '.py':
            default = default[:-3]

        self.my_settings = {}
        try:
            if isinstance(default, str):
                settings = importlib.import_module(default)
            else:
                settings = default
            self.my_settings = self._convert_to_dict(settings)
        except ImportError:
            print("No default settings found")

    def _load_custom(self, settings_name='localsettings.py'):
        '''
        Load the user defined settings, overriding the defaults
        '''
        if isinstance(settings_name, str) and settings_name[-3:] == '.py':
            settings_name = settings_name[:-3]

        new_settings = {}
        try:
            if isinstance(settings_name, str):
                settings = importlib.import_module(settings_name)
            else:
                settings = settings_name
            new_settings = self._convert_to_dict(settings)
        except ImportError:
            print("No override settings found")

        for key in new_settings:
            if key in self.my_settings:
                item = new_settings[key]
                if isinstance(item, dict) and \
                        isinstance(self.my_settings[key], dict):
                    for key2 in item:
                        self.my_settings[key][key2] = item[key2]
                else:
                    self.my_settings[key] = item
            else:
                self.my_settings[key] = new_settings[key]

    def _convert_to_dict(self, setting):
        '''
        Converts a settings file into a dictionary, ignoring python defaults
        @param setting: A loaded setting module
        '''
        the_dict = {}
        set = dir(setting)
        for key in set:
            if key in self.ignore:
                continue
            value = getattr(setting, key)
            the_dict[key] = value

        return the_dict


class FixedConcurrentRotatingFileHandler(ConcurrentRotatingFileHandler):

    def acquire(self):
        """ Acquire thread and file locks.  Re-opening log for 'degraded' mode.
        """
        # handle thread lock
        logging.Handler.acquire(self)
        # Issue a file lock.  (This is inefficient for multiple active threads
        # within a single process. But if you're worried about high-performance,
        # you probably aren't using this log handler.)
        if self.stream_lock:
            # If stream_lock=None, then assume close() was called or something
            # else weird and ignore all file-level locks.
            if self.stream_lock.closed:
                # Daemonization can close all open file descriptors, see
                # https://bugzilla.redhat.com/show_bug.cgi?id=952929
                # Try opening the lock file again.  Should we warn() here?!?
                try:
                    self._open_lockfile()
                except Exception:
                    self.handleError(NullLogRecord())
                    # Don't try to open the stream lock again
                    self.stream_lock = None
                    return
            unlock(self.stream_lock)
            lock(self.stream_lock, LOCK_EX)


class singleton(type):
    """Singleton metaclass to be inherited by child logger
    """
    _instance = {}

    def __new__(cls, *args, **kwargs):
        if cls not in cls._instance:
            cls._instance[cls] = super(singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance[cls]


class LogObject(metaclass=singleton):

    def __init__(self, json=False, name='scrapy-cluster', level='INFO',
                 format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
                 propagate=False):
        self.logger = logging.getLogger(name)
        root = logging.getLogger()
        for log in root.manager.loggerDict.keys():
            root.getChild(log).setLevel(getattr(logging, level, 10))
        self.logger.propagate = propagate
        self.json = json
        self.name = name
        self.format_string = format

    def __wrapper__(self, func: typing.Callable, item):
        @wraps(func)
        def _wraps(*args, **kwargs):
            if len(args) > 2:
                extras = args[1]
            else:
                extras = kwargs.pop("extras", {})
            extras = self.add_extras(extras, item)
            return func(args[0], extra=extras)
        return _wraps

    def set_handler(self, handler):
        handler.setLevel(logging.DEBUG)
        formatter = self._get_formatter(self.json)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.debug("Logging to %s" % handler.__class__.__name__)

    def __getattr__(self, item):
        if item.upper() in logging._nameToLevel:
            func = getattr(self.logger, item)
            return self.__wrapper__(func, item)
        raise AttributeError

    def _get_formatter(self, json):
        if json:
            return jsonlogger.JsonFormatter()
        else:
            return logging.Formatter(self.format_string)

    def add_extras(self, dict, level):
        my_copy = copy.deepcopy(dict)
        if 'level' not in my_copy:
            my_copy['level'] = level
        if 'timestamp' not in my_copy:
            my_copy['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if 'logger' not in my_copy:
            my_copy['logger'] = self.name
        return my_copy


class DecentralizedLogger:
    name = "root"

    setting_wrapper = SettingsWrapper()

    def __init__(self, settings: typing.Union[typing.Dict] = None):
        if isinstance(settings, dict):
            self.settings = settings
        else:
            self.settings = self.setting_wrapper.load(settings, default_settings)

    def set_logger(self, logger=None):
        if logger:
            self.logger = logger
            return None
        my_level = self.settings.get('LOG_LEVEL', 'INFO')
        my_name = self.name
        my_output = self.settings.get('LOG_STDOUT', False)
        my_json = self.settings.get('LOG_JSON', True)
        my_dir = self.settings.get('LOG_DIR', 'logs')
        my_bytes = self.settings.get('LOG_MAX_BYTES', '10MB')
        my_file = "%s.log" % self.name
        my_backups = self.settings.get('LOG_BACKUPS', 5)
        to_kafka = self.settings.get("TO_KAFKA", False)
        to_dweet = self.settings.get("TO_DWEET", False)
        self.logger = LogObject(json=my_json, name=my_name, level=my_level)
        if to_kafka:
            self.logger.set_handler(KafkaHandler(self.settings))
        elif to_dweet:
            self.logger.set_handler(KafkaHandler(self.settings))
        elif my_output:
            self.logger.set_handler(logging.StreamHandler(sys.stdout))
        else:
            os.makedirs(my_dir, exist_ok=True)
            if os.name == "nt":
                handler = FixedConcurrentRotatingFileHandler
            else:
                handler = ConcurrentRotatingFileHandler
            self.logger.set_handler(
                handler(os.path.join(my_dir, my_file),
                        backupCount=my_backups,
                        maxBytes=my_bytes)
            )
