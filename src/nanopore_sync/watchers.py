import asyncio as aio

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
    Integrates watchdog event handling with an asyncio event loop for asynchronous processing.

    Args:
        loop (aio.BaseEventLoop): The asyncio event loop to use for scheduling event callbacks.
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
            return lambda event: self._loop.call_soon_threadsafe(self._loop.create_task, method(event))
        return super().__getattribute__(name)


class NanoporeRunEventHandler(AsyncEventHandler):
    """
    Handles creation events for new nanopore run directories.
    Watches for new run directories and initiates completion monitoring when detected.

    Returns:
        None
    """
    def __init__(self, *args, **kwargs):
        _regexes = [f".*/{CONFIG.run_name_pattern}"]
        super().__init__(*args, regexes=_regexes, **kwargs)

    async def on_created(self, event: DirCreatedEvent):
        watch_run_completion(event.src_path)
        LOGGER.info(f"Detected new run: {event.src_path}")


class NanoporeCompletionEventHandler(AsyncEventHandler):
    """
    Handles completion events for nanopore sequencing runs.
    Watches for file closed events matching the completion signal and triggers synchronization when detected.

    Args:
        observer (ObserverType): The observer instance managing this handler.
        path (str): The path to the run directory being watched.
    """

    def __init__(self, *args, observer: ObserverType, path: str, **kwargs):
        _regexes = [CONFIG.completion_signal_pattern]
        self.path = path
        self._observer = observer
        super().__init__(*args, regexes=_regexes, **kwargs)

    async def on_closed(self, event: FileClosedEvent):
        LOGGER.info(f"Detected completed run: {event.src_path}")
        self._observer.stop()
        self._observer.join()

        await self._loop.run_in_executor(None, sync_run, self.path)


def watch_new_runs():
    """
    Watches the configured source directory for new nanopore run directories.
    Sets up an observer and event handler to detect and process new sequencing runs as they appear.

    Returns:
        None
    """
    observer = Observer()
    loop = aio.new_event_loop()
    handler = NanoporeRunEventHandler(loop=loop)
    observer.schedule(handler, path=CONFIG.source, event_filter=[DirCreatedEvent])
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
    Watches a specific run directory for completion signals indicating the run has finished.
    Sets up an observer and event handler to detect completion events and trigger synchronization.

    Args:
        path (str): The path to the run directory to watch for completion.

    Returns:
        None
    """
    observer = Observer()
    loop = aio.get_running_loop()
    handler = NanoporeCompletionEventHandler(
        loop=loop,
        observer=observer,
        path=path,
    )
    observer.schedule(handler, path=path, event_filter=[FileClosedEvent])
    try:
        observer.start()
        loop.call_soon_threadsafe(loop.run_in_executor, None, observer.join)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
