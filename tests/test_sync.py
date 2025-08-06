import asyncio as aio
from contextlib import suppress
from pathlib import Path

from pytest import mark


@mark.asyncio
async def test_sync(tmp_path: Path) -> None:
    """
    Tests the end-to-end synchronization of a nanopore run directory using the CLI.
    Verifies that all expected files and directories are copied and that the success log message is present.

    Args:
        tmp_path (Path): Temporary directory provided by pytest for test isolation.

    Returns:
        None
    """
    (input := (tmp_path / "input")).mkdir()
    (output := (tmp_path / "output")).mkdir()

    cmdline = [
        *("python", "-m", "nanopore_sync"),
        *("--source", str(input)),
        *("--destination", str(output)),
        "--no-verify",
    ]
    proc = await aio.subprocess.create_subprocess_exec(
        *cmdline,
        stdout=aio.subprocess.PIPE,
        stderr=aio.subprocess.PIPE,
    )

    await aio.sleep(0.5)
    (input / "20231001_1200_run_a_12345678").mkdir()
    await aio.sleep(0.1)
    (input / "20231001_1200_run_a_12345678" / "a.txt").touch()
    (input / "20231001_1200_run_a_12345678" / "b.txt").touch()
    (input / "20231001_1200_run_a_12345678" / "c").mkdir()
    (input / "20231001_1200_run_a_12345678" / "c" / "d.txt").touch()
    await aio.sleep(0.1)
    (input / "20231001_1200_run_a_12345678" / "final_summary.txt").touch()
    await aio.sleep(0.1)

    with suppress(ProcessLookupError):
        proc.terminate()
    await proc.wait()
    _, stderr = await proc.communicate()
    logs = stderr.decode("utf-8")

    # Verify that the expected run directory was detected
    assert f"Detected new run directory: {input / '20231001_1200_run_a_12345678'}" in logs

    # Verify that subdirectories are not detected as new runs
    assert f"Detected new run directory: {input / '20231001_1200_run_a_12345678' / 'c'}" not in logs

    # Verify that the run was synced successfully
    assert "Run '20231001_1200_run_a_12345678' synced successfully." in logs
    assert (output / "20231001_1200_run_a_12345678").exists()
    for path in ["a.txt", "b.txt", "c/d.txt"]:
        assert (output / "20231001_1200_run_a_12345678" / path).exists()
    assert (output / "20231001_1200_run_a_12345678" / "final_summary.txt").exists()
