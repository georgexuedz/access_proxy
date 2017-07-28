#!/usr/bin/env python

import os
import sys
import logging
import logging.handlers
import inspect
from cloghandler import ConcurrentRotatingFileHandler

try:
    import curses
except ImportError:
    curses = None

try:
    import util
except ImportError:
    util = None

_LOGGER_NAME = 'frame_log'
log = logging.getLogger(_LOGGER_NAME)


def _stderr_supports_color():
    color = False
    if curses and hasattr(sys.stderr, 'isatty') and sys.stderr.isatty():
        try:
            curses.setupterm()
            if curses.tigetnum("colors") > 0:
                color = True
        except Exception:
            pass
    return color


def _safe_unicode(s):
    try:
        if isinstance(s, (unicode, type(None))):
            return s
        if not isinstance(s, bytes):
            # Expected bytes, unicode, or None; got type(s)
            return repr(s)
        return s.decode("utf-8")
    except UnicodeDecodeError:
        return repr(s)


class LogFormatter(logging.Formatter):
    DEFAULT_FORMAT = '%(color)s[%(asctime)s.%(msecs)03d %(process)s %(levelname)s %(module)s:%(funcName)s:%(lineno)d]%(end_color)s %(message)s'
    DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    DEFAULT_COLORS = {
        logging.DEBUG: 4,  # Blue
        logging.INFO: 2,  # Green
        logging.WARNING: 3,  # Yellow
        logging.ERROR: 1,  # Red
    }

    def __init__(self, color=True, fmt=DEFAULT_FORMAT,
                 datefmt=DEFAULT_DATE_FORMAT, colors=DEFAULT_COLORS,
                 msg_max_line=30, msg_max_byte=3*1024):
        r"""
        :arg bool color: Enables color support.
        :arg string fmt: Log message format.
          It will be applied to the attributes dict of log records. The
          text between ``%(color)s`` and ``%(end_color)s`` will be colored
          depending on the level if color support is on.
        :arg dict colors: color mappings from logging level to terminal color
          code
        :arg string datefmt: Datetime format.
          Used for formatting ``(asctime)`` placeholder in ``prefix_fmt``.
        :arg msg_max_line: Max lines each message
        :arg msg_max_byte: Max bytes each message

        .. versionchanged:: 3.2

           Added ``fmt`` and ``datefmt`` arguments.
        """
        logging.Formatter.__init__(self, datefmt=datefmt)
        self._fmt = fmt

        self._colors = {}
        if color and _stderr_supports_color():
            # The curses module has some str/bytes confusion in
            # python3.  Until version 3.2.3, most methods return
            # bytes, but only accept strings.  In addition, we want to
            # output these strings with the logging module, which
            # works with unicode strings.  The explicit calls to
            # unicode() below are harmless in python2 but will do the
            # right conversion in python 3.
            fg_color = (curses.tigetstr("setaf") or
                        curses.tigetstr("setf") or "")
            if (3, 0) < sys.version_info < (3, 2, 3):
                fg_color = unicode(fg_color, "ascii")

            for levelno, code in colors.items():
                self._colors[levelno] = unicode(curses.tparm(fg_color, code),
                                                "ascii")
            self._normal = unicode(curses.tigetstr("sgr0"), "ascii")
        else:
            self._normal = ''

        self._msg_max_line = msg_max_line
        self._msg_max_byte = msg_max_byte

    def format(self, record):
        try:
            message = record.getMessage()
            assert isinstance(message, basestring)  # guaranteed by logging
            # Encoding notes:  The logging module prefers to work with character
            # strings, but only enforces that log messages are instances of
            # basestring.  In python 2, non-ascii bytestrings will make
            # their way through the logging framework until they blow up with
            # an unhelpful decoding error (with this formatter it happens
            # when we attach the prefix, but there are other opportunities for
            # exceptions further along in the framework).
            #
            # If a byte string makes it this far, convert it to unicode to
            # ensure it will make it out to the logs.  Use repr() as a fallback
            # to ensure that all byte strings can be converted successfully,
            # but don't do it by default so we don't add extra quotes to ascii
            # bytestrings.  This is a bit of a hacky place to do this, but
            # it's worth it since the encoding errors that would otherwise
            # result are so useless (and tornado is fond of using utf8-encoded
            # byte strings whereever possible).
            record.message = _safe_unicode(message)
        except Exception as e:
            record.message = "Bad message (%r): %r" % (e, record.__dict__)

        record.asctime = self.formatTime(record, self.datefmt)

        if record.levelno in self._colors:
            record.color = self._colors[record.levelno]
            record.end_color = self._normal
        else:
            record.color = record.end_color = ''

        if util:
            record.message = util.cut_str(record.message, self._msg_max_line,
                                          self._msg_max_byte, scar="...")
        formatted = self._fmt % record.__dict__

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            # exc_text contains multiple lines.  We need to _safe_unicode
            # each line separately so that non-utf8 bytes don't cause
            # all the newlines to turn into '\n'.
            lines = [formatted.rstrip()]
            lines.extend(_safe_unicode(ln)
                         for ln in record.exc_text.split('\n'))
            formatted = '\n'.join(lines)
        return formatted.replace("\n", "\n    ")


def init(config):
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(getattr(logging, config.get('level', 'info').upper()))

    channel = config.get('channel', config.get('handler'))
    if channel and channel.lower() == 'rotating_file':
        path = config.get("path", "./")
        module = config.get("module", "unknown")
        filename = os.path.join(path, module + ".log")

        handler = ConcurrentRotatingFileHandler(
            filename=filename,
            maxBytes=config.get('file_max_size', 10*1024*1024),
            backupCount=config.get('file_num_backups', 10))
        handler.setFormatter(LogFormatter(color=False))
        logger.addHandler(handler)

    if channel is None or channel.lower() == 'console':
        # Set up color if we are in a tty and curses is installed
        handler = logging.StreamHandler()
        handler.setFormatter(LogFormatter())
        logger.addHandler(handler)


# for convenient call
log.init = init


# for old version use
def debug(message):
    cur_file, cur_line, cur_func = inspect.getouterframes(
        inspect.currentframe())[1][1:4]
    cur_file = cur_file.split("/")[-1]
    message = "# %s:%s:%s # %s" % (cur_file, cur_func, cur_line, message)
    log.debug(message)


# for old version use
def info(message):
    cur_file, cur_line, cur_func = inspect.getouterframes(
        inspect.currentframe())[1][1:4]
    cur_file = cur_file.split("/")[-1]
    message = "# %s:%s:%s # %s" % (cur_file, cur_func, cur_line, message)
    log.info(message)


# for old version use
def warn(message):
    cur_file, cur_line, cur_func = inspect.getouterframes(
        inspect.currentframe())[1][1:4]
    cur_file = cur_file.split("/")[-1]
    message = "# %s:%s:%s # %s" % (cur_file, cur_func, cur_line, message)
    log.warning(message)


def warning(message):
    cur_file, cur_line, cur_func = inspect.getouterframes(
        inspect.currentframe())[1][1:4]
    cur_file = cur_file.split("/")[-1]
    message = "# %s:%s:%s # %s" % (cur_file, cur_func, cur_line, message)
    log.warning(message)


# for old version use
def error(message):
    cur_file, cur_line, cur_func = inspect.getouterframes(
        inspect.currentframe())[1][1:4]
    cur_file = cur_file.split("/")[-1]
    message = "# %s:%s:%s # %s" % (cur_file, cur_func, cur_line, message)
    log.error(message)


# for old version use
def bt(message=None):
    import traceback
    log.error(str(traceback.format_exc()))


if __name__ == '__main__':

    def logging_config():
        config = {
            'module': 'access',
            "path": "/data/logs/service/access",
            'level': 'debug',    # debug|info|warning|error|none
            "level": "DEBUG",
            "handler": "rotating_file"
        }

        return config

    init(logging_config())

    log.debug('debug log')
    log.error('error log')
    log.info('info log')
    log.warn('warn log')
    log.warning('warning log')
