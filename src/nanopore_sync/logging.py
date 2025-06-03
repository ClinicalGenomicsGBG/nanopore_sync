"""
Configures and provides a logger for the nanopore sync application.
Sets up logging with a standard format and exposes a module-level LOGGER object.
"""

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

LOGGER = logging.getLogger(__name__)
