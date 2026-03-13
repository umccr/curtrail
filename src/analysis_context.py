from dataclasses import dataclass, field
from datetime import date
from typing import List, Literal, Tuple

"""
The context in which all analysis will be performed. These predicates
should be set as narrow as possible for the analysis - as they enable
the system to avoid ingesting unneeded data. i.e. if an incident/cost
has occurred in only one account - then listing it means that no other
data from other accounts will be loaded.
"""


@dataclass()
class AnalysisContext:

    # a list of accounts to filter to, or * to mean all accounts found
    accounts: List[str] | Literal["*"] = field(init=False, default="*")

    # a list of regions to filter to, or * to mean all regions found
    regions: List[str] | Literal["*"] = field(init=False, default="*")

    # a tuple of start/end date (inclusive)
    days_inclusive: Tuple[date, date] = field(init=False)
