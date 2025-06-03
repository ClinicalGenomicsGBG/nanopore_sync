import rich_click as click
from pydanclick import from_pydantic

from .config import Config, set_global_config
from .watchers import watch_new_runs


@click.command(context_settings={"show_default": True})
@from_pydantic(Config)
def main(config: Config):
    """
    Starts the nanopore sync application using the provided configuration.
    Sets up global configuration and begins watching for new sequencing runs.

    Args:
        config (Config): The configuration object for the application.

    Returns:
        None
    """
    set_global_config(config)
    watch_new_runs()
