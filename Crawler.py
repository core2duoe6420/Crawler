# encoding=utf-8

import requesocks as requests
import threading
import logging


_logger = logging.getLogger("Crawler")
_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s][%(thread)d]: %(message)s')
_loghandler = logging.StreamHandler()
_loghandler.setFormatter(_formatter)
_logger.addHandler(_loghandler)

_sem = threading.Semaphore(5)


def _do_crawl(url, method, event=None, **kwargs):
    _logger.info("start to crawl '%s'." % url)
    try:
        retry_time = 0
        while retry_time < 5:
            try:
                result = method(url, **kwargs)
                _logger.info("succeed in crawling '%s'." % url)
                return result
            except requests.exceptions.Timeout:
                _logger.warning("crawl '%s' timeout, retry %d times." % (url, retry_time))
                retry_time += 1
        _logger.error("crawl '%s' max retry time." % url)
        return None
    finally:
        if event is not None:
            _sem.release()
            event.set()


def _async_crawl(url, method, daemon=False, handler=None, handler_args=None, **kwargs):

    def thread_func():
        result = _do_crawl(url, method, event, **kwargs)
        if handler is not None:
            if handler_args is None:
                handler(result)
            else:
                handler(result, handler_args)

    event = threading.Event()
    _sem.acquire()
    t = threading.Thread(target=thread_func)
    t.setDaemon(daemon)
    t.start()
    return event


def _sync_crawl(url, method, **kwargs):
    return _do_crawl(url, method, **kwargs)


def get(url, async=True, daemon=False, handler=None, handler_args=None, **kwargs):
        if async:
            return _async_crawl(url, requests.get, daemon, handler, handler_args, **kwargs)
        else:
            return _sync_crawl(url, requests.get, **kwargs)


def post(url, async=True, daemon=False, handler=None, handler_args=None, **kwargs):
    if async:
        return _async_crawl(url, requests.post, daemon, handler, handler_args, **kwargs)
    else:
        return _sync_crawl(url, requests.post)


