
import asyncio as aio
import re
from functools import cache
from pathlib import Path

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
    DirModifiedEvent,
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
from watchdog.observers import Observer

from .config import CONFIG
from .logging import LOGGER
from .sync import sync_run


@cache
def _extract_run_name(path: str) -> str | None:
    if (match := re.search(CONFIG.run_name_pattern, path)) is None:
        return None
    return match.group(0)


class AsyncEventHandler(RegexMatchingEventHandler):
    """
    An asyncio-compatible event handler for filesystem events.
    Integrates watchdog event handling with an asyncio event loop for asynchronous processing.

    Args:
        loop (aio.BaseEventLoop): The asyncio event loop to use for scheduling event callbacks.
    """

    def __init__(self, *args, **kwargs):
        self._loop = aio.get_event_loop()
        self._tasks = []
        super().__init__(*args, **kwargs)

    def _create_task(self, coro):
        task = self._loop.create_task(coro)
        self._tasks.append(task)
        task.add_done_callback(self._tasks.remove)
        return task

    def cancel_tasks(self):
        """
        Cancels all currently running tasks in the event handler.
        This is useful for cleanup when stopping the observer.
        """
        for task in self._tasks:
            LOGGER.debug(f"Cancelling task: {task}")
            task.cancel()
        self._tasks.clear()

    async def on_any_event(self, event: FileSystemEvent): ...
    async def on_moved(self, event: FileMovedEvent | DirMovedEvent): ...
    async def on_created(self, event: FileCreatedEvent | DirCreatedEvent): ...
    async def on_deleted(self, event: FileDeletedEvent | DirDeletedEvent): ...
    async def on_modified(self, event: FileModifiedEvent | DirModifiedEvent): ...
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
            return lambda event: self._loop.call_soon_threadsafe(self._create_task, method(event))
        return super().__getattribute__(name)


class RunDirectoryHandler(AsyncEventHandler):
    _new_run: aio.Condition
    _run_completion: dict[str, aio.Condition]
    _run_modified: dict[str, aio.Condition]

    def __init__(self, new_run_condition: aio.Condition, run_completion_conditions: dict[str, aio.Condition]):
        self._new_run = new_run_condition
        self._run_completion = run_completion_conditions
        self._run_modified = {}
        super().__init__(regexes=[f".*/{CONFIG.run_name_pattern}$"])

    async def _do_sync(self, path: str):
        if (run_name := _extract_run_name(path)) is None:
            return
        LOGGER.info(f"Detected new run: '{run_name}'")
        self._run_completion[run_name] = aio.Condition()
        self._run_modified[run_name] = aio.Condition()
        async with self._new_run:
            self._new_run.notify_all()
        async with self._run_completion[run_name]:
            await self._run_completion[run_name].wait()

        LOGGER.info(f"Run completion detected for '{run_name}'")
        LOGGER.infog(f"Waiting {CONFIG.completion_delay} seconds before syncing run '{run_name}'")
        await aio.sleep(CONFIG.completion_delay)
        await self._loop.run_in_executor(None, sync_run, Path(path))

    async def on_any_event(self, event: FileSystemEvent):
        LOGGER.debug(f"RunDirectoryHandler received event: {event.event_type} on {event.src_path}")

    async def on_moved(self, event: DirMovedEvent):
        await self._do_sync(event.dest_path)

    async def on_created(self, event: DirCreatedEvent):
        await self._do_sync(event.src_path)



class RunCompletionHandler(AsyncEventHandler):
    _new_run: aio.Condition
    _run_completion: dict[str, aio.Condition]

    def __init__(self, new_run_condition: aio.Condition, run_completion_conditions: dict[str, aio.Condition]):
        self._new_run = new_run_condition
        self._run_completion = run_completion_conditions
        super().__init__(regexes=[f".*/{CONFIG.run_name_pattern}/{CONFIG.completion_signal_pattern}$"])

    async def _check_completion(self, path: str):
        if (run_name := _extract_run_name(path)) is None:
            return

        async with self._new_run:
            await self._new_run.wait_for(lambda: run_name in self._run_completion)

        async with self._run_completion[run_name]:
            self._run_completion[run_name].notify_all()


    async def on_any_event(self, event: FileSystemEvent):
        LOGGER.debug(f"RunCompletionHandler received event: {event.event_type} on {event.src_path}")

    async def on_moved(self, event: FileMovedEvent):
        await self._check_completion(event.dest_path)

    async def on_created(self, event: FileCreatedEvent):
        await self._check_completion(event.src_path)

    async def on_closed(self, event: FileClosedEvent):
        await self._check_completion(event.src_path)

async def watch_new_runs() -> None:
    observer = Observer()
    new_run_condition = aio.Condition()
    run_completion_conditions = {}

    run_directory_handler = RunDirectoryHandler(new_run_condition, run_completion_conditions)
    run_completion_handler = RunCompletionHandler(new_run_condition, run_completion_conditions)
    observer.schedule(run_directory_handler, path=CONFIG.source, event_filter=[DirCreatedEvent, DirMovedEvent], recursive=True)
    observer.schedule(run_completion_handler, path=CONFIG.source, event_filter=[FileClosedEvent, FileMovedEvent, FileCreatedEvent], recursive=True)

    loop = aio.get_running_loop()
    observer.start()
    try:
        await loop.run_in_executor(None, observer.join)
    except aio.CancelledError:
        LOGGER.info("Interrupted, stop watching for new runs")
        observer.stop()
        await loop.run_in_executor(None, observer.join)
        run_completion_handler.cancel_tasks()
        run_directory_handler.cancel_tasks()
