from dataclasses import dataclass

from curtrail.base_config import BaseConfig
from curtrail.source_filter import SourceFilter


@dataclass(init=False)
class CliContextObj:
    """Combined context: infrastructure config + client-supplied query filter."""

    base_config: BaseConfig = None

    source_filter: SourceFilter = None
