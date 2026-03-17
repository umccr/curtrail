# Mapping of short region aliases to canonical AWS region names.
# Aliases are case-insensitive and allow convenient CLI usage, e.g. "apse2" -> "ap-southeast-2".

_SHORTNAMES: dict[str, str] = {
    # US East
    "use1": "us-east-1",
    "use2": "us-east-2",
    # US West
    "usw1": "us-west-1",
    "usw2": "us-west-2",
    # Canada
    "cac1": "ca-central-1",
    "caw1": "ca-west-1",
    # South America
    "sae1": "sa-east-1",
    # Europe
    "euw1": "eu-west-1",
    "euw2": "eu-west-2",
    "euw3": "eu-west-3",
    "euc1": "eu-central-1",
    "euc2": "eu-central-2",
    "eun1": "eu-north-1",
    "eus1": "eu-south-1",
    "eus2": "eu-south-2",
    # Middle East
    "mes1": "me-south-1",
    "mec1": "me-central-1",
    # Africa
    "afs1": "af-south-1",
    # Asia Pacific
    "apse1": "ap-southeast-1",
    "apse2": "ap-southeast-2",
    "apse3": "ap-southeast-3",
    "apse4": "ap-southeast-4",
    "apne1": "ap-northeast-1",
    "apne2": "ap-northeast-2",
    "apne3": "ap-northeast-3",
    "aps1": "ap-south-1",
    "aps2": "ap-south-2",
    "ape1": "ap-east-1",
    # Israel
    "ilc1": "il-central-1",
}

# Reverse mapping: canonical name -> short alias (for reference/display)
_CANONICAL: dict[str, str] = {v: k for k, v in _SHORTNAMES.items()}


def expand_region(region: str) -> str:
    """Return the canonical AWS region name for *region*.

    If *region* is already a canonical name (contains a hyphen) or the special
    wildcard ``"*"`` it is returned unchanged.  Otherwise the value is looked up
    in the short-name table (case-insensitively).  An unknown alias raises
    ``ValueError``.
    """
    if "-" in region:
        return region
    canonical = _SHORTNAMES.get(region.lower())
    if canonical is None:
        known = ", ".join(sorted(_SHORTNAMES))
        raise ValueError(
            f"Unknown region alias '{region}'. " f"Known short names: {known}"
        )
    return canonical


def expand_regions(regions: list[str]) -> list[str]:
    """Expand a list of region specifiers, resolving any short aliases."""
    return [expand_region(r) for r in regions]
