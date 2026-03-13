from dataclasses import dataclass
from typing import Optional

from analysis_context import AnalysisContext
from analysis_source_billing import AnalysisSourceCur
from analysis_source_log import AnalysisSourceCloudTrail


@dataclass
class AppContext:
    analysis: AnalysisContext

    bill: Optional[AnalysisSourceCur]

    log: Optional[AnalysisSourceCloudTrail]

    geolite2_path: Optional[str]
