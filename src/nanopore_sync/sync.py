from pathlib import Path
from shutil import copytree, copy

from .config import CONFIG
from .logging import LOGGER


def _dir_size(path: Path) -> int:
    """
    Computes the total size in bytes of all files within a directory and its subdirectories.
    Returns the sum of the sizes of all files found under the given path.

    Args:
        path (Path): The root directory whose file sizes will be summed.

    Returns:
        int: The total size in bytes of all files in the directory.
    """
    return sum(f.stat().st_size for f in path.glob("**/*") if f.is_file())


def sync_run(source: Path) -> None:
    """
    Synchronizes a sequencing run directory from the source to the configured destination.
    Handles copying, existence checks, error logging, and optional size verification.

    Args:
        source (Path): The path to the source run directory.

    Returns:
        None
    """
    destination = Path(CONFIG.destination)
    source = Path(source)

    try:
        LOGGER.info(f"Syncing run '{source.name}' to '{destination}'...")
        copytree(source, destination / source.name, copy_function=copy)
    except FileExistsError:
        LOGGER.warning(f"Run '{source.name}' already exists in '{destination}'.")
        return
    except Exception as exc:
        LOGGER.error(f"Unable to copy run '{source.name}': {exc}")
        return

    if CONFIG.verify and (_ssize := _dir_size(source)) != (_dsize := _dir_size(destination / source.name)):
        LOGGER.warning(f"Size mismatch for run '{source.name}': source size {_ssize}, destination size {_dsize}.")
        return

    LOGGER.info(f"Run '{source.name}' synced successfully.")
