import asyncio as aio
from pathlib import Path
import re

from watchdog.events import (
    EVENT_TYPE_CLOSED,
    EVENT_TYPE_CLOSED_NO_WRITE,
    EVENT_TYPE_CREATED,
    EVENT_TYPE_DELETED,
    EVENT_TYPE_MODIFIED,
    EVENT_TYPE_MOVED,
    EVENT_TYPE_OPENED,
    DirCreatedEvent,
    DirDeletedEvent,
    DirMovedEvent,
    FileClosedEvent,
    FileClosedNoWriteEvent,
    FileCreatedEvent,
    FileDeletedEvent,
    FileModifiedEvent,
    FileMovedEvent,
    FileOpenedEvent,
    FileSystemEvent,
    RegexMatchingEventHandler,
)
from watchdog.observers import Observer, ObserverType

from .config import CONFIG
from .logging import LOGGER
from .sync import sync_run


class AsyncEventHandler(RegexMatchingEventHandler):
    """
    An asyncio-compatible event handler for filesystem events.
    """

    def __init__(self, *args, loop: aio.BaseEventLoop, **kwargs):
        self._loop = loop
        super().__init__(*args, **kwargs)

    async def on_any_event(self, event: FileSystemEvent): ...
    async def on_moved(self, event: FileMovedEvent | DirMovedEvent): ...
    async def on_created(self, event: FileCreatedEvent | DirCreatedEvent): ...
    async def on_deleted(self, event: FileDeletedEvent | DirDeletedEvent): ...
    async def on_modified(self, event: FileModifiedEvent): ...
    async def on_closed(self, event: FileClosedEvent): ...
    async def on_closed_no_write(self, event: FileClosedNoWriteEvent): ...
    async def on_opened(self, event: FileOpenedEvent): ...

    def __getattribute__(self, name):
        if name in (
            f"on_{event_type}"
            for event_type in (
                "any_event",
                EVENT_TYPE_MODIFIED,
                EVENT_TYPE_MOVED,
                EVENT_TYPE_CREATED,
                EVENT_TYPE_DELETED,
                EVENT_TYPE_CLOSED,
                EVENT_TYPE_CLOSED_NO_WRITE,
                EVENT_TYPE_OPENED,
            )
        ):
            method = object.__getattribute__(self, name)
            return lambda event: self._loop.call_soon_threadsafe(
                self._loop.create_task, method(event)
            )
        return super().__getattribute__(name)


class NanoporeRunEventHandler(AsyncEventHandler):
    """
    Detects new run directories and starts a per-run completion watcher.
    """

    def __init__(self, *args, **kwargs):
        _regexes = [f".*/{CONFIG.run_name_pattern}$"]
        super().__init__(*args, regexes=_regexes, **kwargs)

    async def on_created(self, event: DirCreatedEvent):
        watch_run_completion(event.src_path)
        LOGGER.info(f"Detected new run directory: {event.src_path}")


class NanoporeCompletionEventHandler(AsyncEventHandler):
    """
    Detects completion of a run by noticing final_summary*.txt arriving/closing.
    Robust to: create, close, close-no-write, in-place rename, or cross-dir moves.
    Also includes a tiny poller as a safety net.
    """

    def __init__(self, *args, observer: ObserverType, path: str, **kwargs):
        # Allow all events through; we filter by regex ourselves to avoid dropping unpaired moves.
        _regexes = [r".*"]
        self._sig_re = re.compile(CONFIG.completion_signal_pattern)
        self._done = False
        self.path = Path(path)
        self._observer = observer
        super().__init__(*args, regexes=_regexes, **kwargs)
        self._loop.create_task(self._poll_for_final_summary())

    async def _poll_for_final_summary(self, interval: float = 2.0):
        while not self._done:
            for p in self.path.glob("final_summary*.txt"):
                if self._sig_re.search(str(p)):
                    await self._do_sync(str(p))
                    return
            await aio.sleep(interval)

    async def _do_sync(self, matched_path: str):
        if self._done:
            return
        self._done = True
        LOGGER.info(f"Detected completed run: {matched_path}")
        self._observer.stop()
        await self._loop.run_in_executor(None, self._observer.join)
        await self._loop.run_in_executor(None, sync_run, self.path)

    async def on_created(self, event: FileCreatedEvent):
        if self._sig_re.search(event.src_path):
            await self._do_sync(event.src_path)

    async def on_closed(self, event: FileClosedEvent):
        if self._sig_re.search(event.src_path):
            await self._do_sync(event.src_path)

    async def on_closed_no_write(self, event: FileClosedNoWriteEvent):
        if self._sig_re.search(event.src_path):
            await self._do_sync(event.src_path)

    async def on_moved(self, event: FileMovedEvent):
        dest = getattr(event, "dest_path", "") or ""
        if (dest and self._sig_re.search(dest)) or self._sig_re.search(event.src_path):
            await self._do_sync(dest if dest and self._sig_re.search(dest) else event.src_path)

    async def on_any_event(self, event):
        dest = getattr(event, "dest_path", None)
        LOGGER.debug(f"Detected '{event.__class__.__name__}' src='{event.src_path}' dest='{dest}'")


def watch_new_runs():
    """
    Watch the source tree for new run directories.
    """
    observer = Observer()
    loop = aio.new_event_loop()
    handler = NanoporeRunEventHandler(loop=loop)
    observer.schedule(handler, path=CONFIG.source, event_filter=[DirCreatedEvent], recursive=True)
    LOGGER.info(f"[discovery] watching '{CONFIG.source}' recursively for new runs")
    try:
        observer.start()
        loop.run_until_complete(loop.run_in_executor(None, observer.join))
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
    finally:
        loop.close()


def watch_run_completion(path: str):
    """
    Watch a single run directory for completion (final_summary*.txt).
    """
    observer = Observer()
    loop = aio.get_running_loop()
    handler = NanoporeCompletionEventHandler(
        loop=loop,
        observer=observer,
        path=path,
    )
    # Listen for all plausible completion signals; some backends will emit only a subset.
    observer.schedule(
        handler,
        path=path,
        event_filter=[FileCreatedEvent, FileClosedEvent, FileClosedNoWriteEvent, FileMovedEvent],
    )
    LOGGER.info(f"[completion] watching run dir '{path}' (non-recursive)")
    try:
        observer.start()
        loop.call_soon_threadsafe(loop.run_in_executor, None, observer.join)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
