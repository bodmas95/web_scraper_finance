"""
Global worker pools for parallel processing.

Provides a ProcessPoolExecutor for CPU-bound work (PDF parsing)
and a ThreadPoolExecutor for I/O-bound work (LLM calls, HTTP, MongoDB).

Pool sizes are controlled by [PERFORMANCE] in config.ini.
Set enable_parallel = false to run everything synchronously.
"""

import os
import atexit
import logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

_log = logging.getLogger(__name__)

_process_pool: ProcessPoolExecutor | None = None
_thread_pool: ThreadPoolExecutor | None = None

_MAX_CPU_WORKERS: int | None = None
_MAX_IO_WORKERS: int | None = None
_PARALLEL_ENABLED: bool | None = None


def _load_settings():
    global _MAX_CPU_WORKERS, _MAX_IO_WORKERS, _PARALLEL_ENABLED
    if _PARALLEL_ENABLED is not None:
        return
    try:
        from config.config import load_config
        cfg = load_config()
        cpus = os.cpu_count() or 4
        _MAX_CPU_WORKERS = cfg.getint("PERFORMANCE", "max_workers_cpu", fallback=min(4, cpus))
        _MAX_IO_WORKERS = cfg.getint("PERFORMANCE", "max_workers_io", fallback=min(8, cpus * 2))
        _PARALLEL_ENABLED = cfg.getboolean("PERFORMANCE", "enable_parallel", fallback=True)
    except Exception:
        _MAX_CPU_WORKERS = min(4, os.cpu_count() or 4)
        _MAX_IO_WORKERS = min(8, (os.cpu_count() or 4) * 2)
        _PARALLEL_ENABLED = True


def get_process_pool() -> ProcessPoolExecutor:
    global _process_pool
    _load_settings()
    if _process_pool is None:
        _process_pool = ProcessPoolExecutor(max_workers=_MAX_CPU_WORKERS)
        _log.info("ProcessPoolExecutor started with %d workers", _MAX_CPU_WORKERS)
    return _process_pool


def get_thread_pool() -> ThreadPoolExecutor:
    global _thread_pool
    _load_settings()
    if _thread_pool is None:
        _thread_pool = ThreadPoolExecutor(max_workers=_MAX_IO_WORKERS)
        _log.info("ThreadPoolExecutor started with %d workers", _MAX_IO_WORKERS)
    return _thread_pool


def submit_io(fn, *args, **kwargs):
    """Submit I/O-bound work to the thread pool. Returns a Future."""
    _load_settings()
    if not _PARALLEL_ENABLED:
        return _SyncFuture(fn(*args, **kwargs))
    return get_thread_pool().submit(fn, *args, **kwargs)


def submit_cpu(fn, *args, **kwargs):
    """Submit CPU-bound work to the process pool. Returns a Future."""
    _load_settings()
    if not _PARALLEL_ENABLED:
        return _SyncFuture(fn(*args, **kwargs))
    return get_process_pool().submit(fn, *args, **kwargs)


def parallel_map_io(fn, items, max_workers=None):
    """
    Run fn(item) for each item in parallel using the thread pool.
    Returns results in the same order as items.
    """
    _load_settings()
    if not _PARALLEL_ENABLED or len(items) <= 1:
        return [fn(item) for item in items]

    pool = get_thread_pool()
    futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
    results = [None] * len(items)
    for future in as_completed(futures):
        idx = futures[future]
        results[idx] = future.result()
    return results


def parallel_map_cpu(fn, items, max_workers=None):
    """
    Run fn(item) for each item in parallel using the process pool.
    Returns results in the same order as items.
    """
    _load_settings()
    if not _PARALLEL_ENABLED or len(items) <= 1:
        return [fn(item) for item in items]

    pool = get_process_pool()
    futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
    results = [None] * len(items)
    for future in as_completed(futures):
        idx = futures[future]
        results[idx] = future.result()
    return results


def shutdown():
    """Shut down all worker pools."""
    global _process_pool, _thread_pool
    if _process_pool:
        _process_pool.shutdown(wait=False, cancel_futures=True)
        _process_pool = None
    if _thread_pool:
        _thread_pool.shutdown(wait=False, cancel_futures=True)
        _thread_pool = None
    _log.info("Worker pools shut down")


class _SyncFuture:
    """Drop-in Future replacement for synchronous execution."""
    def __init__(self, value):
        self._value = value
        self._exception = None

    def result(self, timeout=None):
        if self._exception:
            raise self._exception
        return self._value

    def done(self):
        return True


atexit.register(shutdown)
