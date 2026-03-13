from dataclasses import dataclass, field
from typing import List, Literal, Optional, Tuple
from datetime import date

"""
The source of analysis data.
"""
@dataclass()
class AnalysisSourceCloudTrail:

    s3_bucket: str

    s3_prefix: str

    organisation: Optional[str] = None
