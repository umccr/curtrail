import tomllib
from dataclasses import field, dataclass
from pathlib import Path
from typing import List, Optional

from curtrail.source_bill_cur import SourceBillCUR
from curtrail.source_bill_cur_test_data import SourceBillCURTestData
from curtrail.source_bill_ica import SourceBillIca
from curtrail.source_log_cloudtrail import SourceLogCloudTrail
from curtrail.source_log_cloudtrail_test_data import SourceLogCloudTrailTestData


@dataclass
class BaseConfig:
    """Infrastructure config: where to find data and supporting resources.

    Loaded from a TOML file or built from CLI arguments. Does not contain
    any query parameters (date range, accounts) — those come from the client
    via SourceFilter.

    Both bill_aws and bill_ica may be configured simultaneously. Callers
    choose which billing source to use at query time.
    """

    bill_aws: List[SourceBillCUR] = field(default_factory=list)

    bill_ica: List[SourceBillIca] = field(default_factory=list)

    logs: List[SourceLogCloudTrail] = field(default_factory=list)

    geolite2_path: Optional[str] = None


def load_base_config(config_path: Path = Path("curtrail.toml")) -> BaseConfig:
    """Load infrastructure config (sources and geo) from a TOML file.

    The returned BaseConfig holds the data sources and supporting settings.
    Callers are responsible for creating a SourceFilter (date range, accounts, regions)
    and combining the two for later use.
    """
    with open(config_path, "rb") as f:
        cfg = tomllib.load(f)

    bill_aws = [
        (
            SourceBillCURTestData(data_prefix=b["data_prefix"])
            if b.get("test_data_loader", False)
            else SourceBillCUR(data_prefix=b["data_prefix"])
        )
        for b in cfg.get("bill_aws", [])
    ]

    bill_ica = [
        SourceBillIca(data_prefix=b["data_prefix"]) for b in cfg.get("bill_ica", [])
    ]

    logs = [
        (
            SourceLogCloudTrailTestData(
                data_prefix=l["data_prefix"],
                organisation=l.get("organisation"),
            )
            if l.get("test_data_loader", False)
            else SourceLogCloudTrail(
                data_prefix=l["data_prefix"],
                organisation=l.get("organisation"),
            )
        )
        for l in cfg.get("log", [])
    ]

    geolite2_path = cfg.get("geo", {}).get("geolite2_path")

    return BaseConfig(
        bill_aws=bill_aws,
        bill_ica=bill_ica,
        logs=logs,
        geolite2_path=geolite2_path,
    )
