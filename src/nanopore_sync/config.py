

from pydantic import BaseModel, Field


class CONFIG:
    source: str
    destination: str
    run_name_pattern: str
    completion_signal_pattern: str
    verify: bool


class Config(BaseModel):
    """
    Represents the configuration settings for the nanopore sync application.
    Stores source and destination paths, run name and completion signal patterns, and verification options.

    Attributes:
        source (str): Path to directory containing nanopore runs.
        destination (str): Path to directory where runs will be synced.
        run_name_pattern (str): Regex pattern to match nanopore run names.
        completion_signal_pattern (str): Regex pattern to match the completion signal file.
        verify (bool): Checks the total directory size after copy.
    """
    class Config:
        validate_assignment = True
        extra = "forbid"

    source: str = Field(
        ...,
        description="Path to directory containing nanopore runs",
        required=True,
    )
    destination: str = Field(
        ...,
        description="Path to directory where runs will be synced",
        required=True,
    )
    run_name_pattern: str = Field(
        r"[0-9]{8}_[0-9]{4}_[^_]+_[^_]+_[a-f0-9]{8}",
        description="Regex pattern to match nanopore run names",
    )
    completion_signal_pattern: str = Field(
        r".*/final_summary.*\.txt$",
        description="Regex pattern to match the completion signal file",
    )
    verify: bool = Field(
        True,
        description="Checks the total directory size after copy",
    )


def set_global_config(config: "Config"):
    """
    Updates the global configuration object with values from the provided Config instance.
    Copies all relevant configuration fields to the global CONFIG object for use throughout the application.

    Args:
        config (Config): The configuration object containing new settings.

    Returns:
        None
    """
    global CONFIG
    CONFIG.source = config.source
    CONFIG.destination = config.destination
    CONFIG.run_name_pattern = config.run_name_pattern
    CONFIG.completion_signal_pattern = config.completion_signal_pattern
    CONFIG.verify = config.verify
