import rich_click as click
from pydanclick import from_pydantic

from .config import Config, set_global_config
from .watchers import watch_new_runs
from .logging import LOGGER


@click.command(context_settings={"show_default": True})
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set the logging level for the application."
)
@from_pydantic(Config)
def main(config: Config, log_level: str) -> None:
    """
    Starts the nanopore sync application using the provided configuration.
    Sets up global configuration and begins watching for new sequencing runs.

    Args:
        config (Config): The configuration object for the application.

    Returns:
        None
    """
    set_global_config(config)
    LOGGER.setLevel(log_level)
    watch_new_runs()
