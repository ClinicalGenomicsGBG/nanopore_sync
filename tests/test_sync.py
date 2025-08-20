import asyncio as aio
from contextlib import suppress
from pathlib import Path
from typing import AsyncGenerator

from pytest import mark
from pytest_asyncio import fixture as async_fixture


@async_fixture(scope="function")
async def nanopore_sync(tmp_path: Path) -> AsyncGenerator[tuple[Path, Path, aio.subprocess.Process], None]:
    """Fixture for running nanopore-sync, providing temporary input and output directories.

    Returns:
        tuple[Path, Path, aio.subprocess.Process]: A tuple containing the input and output directories and the subprocess handle.
    """
    (input := (tmp_path / "input")).mkdir()
    (output := (tmp_path / "output")).mkdir()

    cmdline = [
        *("python", "-m", "nanopore_sync"),
        *("--source", str(input)),
        *("--destination", str(output)),
        "--log-level", "DEBUG",
        "--verify",
    ]
    proc = await aio.subprocess.create_subprocess_exec(
        *cmdline,
        stdout=aio.subprocess.PIPE,
        stderr=aio.subprocess.PIPE,
    )

    # Allow some time for the process to start
    await aio.sleep(0.5)
    yield input, output, proc

    # Ensure the process is terminated after the test
    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()

@mark.asyncio
async def test_sync(nanopore_sync) -> None:
    input, output, proc = nanopore_sync

    (input / "20231001_1200_run_a_12345678").mkdir()
    # FIXME: Use a more realistic run directory structure
    (input / "20231001_1200_run_a_12345678" / "a.txt").write_text("SOME DATA")
    (input / "20231001_1200_run_a_12345678" / "b.txt").write_text("SOME MORE DATA")
    (input / "20231001_1200_run_a_12345678" / "c").mkdir()
    (input / "20231001_1200_run_a_12345678" / "c" / "d.txt").write_text("EVEN MORE DATA")
    (input / "20231001_1200_run_a_12345678" / "final_summary.txt").touch()
    await aio.sleep(.1)

    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()
    _, stderr = await proc.communicate()
    logs = stderr.decode("utf-8")

    # Verify that the expected run directory was detected
    assert "Detected new run: '20231001_1200_run_a_12345678'" in logs, logs

    # Verify that the run was synced successfully
    assert "Run '20231001_1200_run_a_12345678' synced successfully." in logs, logs
    assert "Size missmatch for run '20231001_1200_run_a_12345678'" not in logs, logs
    assert (output / "20231001_1200_run_a_12345678").exists()
    for path in ["a.txt", "b.txt", "c/d.txt", "final_summary.txt"]:
        assert (output / "20231001_1200_run_a_12345678" / path).exists()


@mark.asyncio
async def test_sync_rename(nanopore_sync) -> None:
    input, output, proc = nanopore_sync

    (input / "20231001_1200_run_a_12345678").mkdir()
    (input / "20231001_1200_run_a_12345678" / "tmp_final_summary.txt").touch()
    (input / "20231001_1200_run_a_12345678" / "tmp_final_summary.txt").rename(
        input / "20231001_1200_run_a_12345678" / "final_summary.txt"
    )
    await aio.sleep(.1)

    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()
    _, stderr = await proc.communicate()
    logs = stderr.decode("utf-8")

    # Verify that the expected run directory was detected
    assert "Detected new run: '20231001_1200_run_a_12345678'" in logs, logs

    # Verify that the run was synced successfully
    assert "Run '20231001_1200_run_a_12345678' synced successfully." in logs, logs
    assert (output / "20231001_1200_run_a_12345678" / "final_summary.txt").exists()

@mark.asyncio
async def test_sync_move_dir(nanopore_sync) -> None:
    input, output, proc = nanopore_sync

    (input / "20231001_1200_run_a_12345678_TMP").mkdir()
    (input / "20231001_1200_run_a_12345678_TMP" / "final_summary.txt").touch()
    (input / "20231001_1200_run_a_12345678_TMP").rename(input / "20231001_1200_run_a_12345678")
    await aio.sleep(.1)

    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()
    _, stderr = await proc.communicate()
    logs = stderr.decode("utf-8")

    # Verify that the expected run directory was detected
    assert "Detected new run: '20231001_1200_run_a_12345678'" in logs, logs

    # Verify that the run was synced successfully
    assert "Run '20231001_1200_run_a_12345678' synced successfully." in logs, logs
    assert (output / "20231001_1200_run_a_12345678" / "final_summary.txt").exists()
    assert False, logs

@mark.asyncio
async def test_sync_move_from_outside(nanopore_sync, tmp_path) -> None:
    input, output, proc = nanopore_sync
    (external := tmp_path / "external").mkdir()

    (external / "final_summary.txt").touch()
    (input / "20231001_1200_run_a_12345678").mkdir()
    await aio.sleep(.1)
    (external / "final_summary.txt").rename(input / "20231001_1200_run_a_12345678" / "final_summary.txt")
    await aio.sleep(.1)

    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()
    _, stderr = await proc.communicate()
    logs = stderr.decode("utf-8")

    # Verify that the expected run directory was detected
    assert "Detected new run: '20231001_1200_run_a_12345678'" in logs, logs

    # Verify that the run was synced successfully
    assert "Run '20231001_1200_run_a_12345678' synced successfully." in logs, logs
    assert (output / "20231001_1200_run_a_12345678" / "final_summary.txt").exists()