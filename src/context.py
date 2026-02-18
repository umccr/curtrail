from dataclasses import dataclass, field
from typing import List, Literal, Optional, Tuple
from datetime import date

type DaysRangeInclusive = Tuple[date,date]|Literal["*"]

"""
The context in which all analysis will be performed. These predicates
should be set as narrow as possible for the analysis - as they enable
the system to avoid ingesting unneeded data. i.e. if an incident/cost
has occurred in only one account - then listing it means that no other
data from other accounts will be loaded.
"""
@dataclass()
class AnalysisContext:

    # a list of accounts to filter to, or * to mean all accounts
    accounts: List[str]|Literal["*"] = field(init=False, default="*")

    # a list of regions to filter to, or * to mean all regions
    regions: List[str]|Literal["*"] = field(init=False, default="*")

    # either a tuple of start/end date (inclusive) or * to mean all dates
    days_inclusive: DaysRangeInclusive = field(init=False, default="*")

    organisation: Optional[str] = None
