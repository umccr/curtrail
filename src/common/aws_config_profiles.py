import configparser
import re
from functools import lru_cache
from pathlib import Path

_ROLE_ARN_ACCOUNT_RE = re.compile(r"arn:aws:iam::(\d{12}):")


@lru_cache(maxsize=None)
def account_id_to_profile(config_path: Path) -> dict[str, str]:
    """
    Parse ~/.aws/config and return a mapping of account id -> profile name.
    Where multiple profiles share an account id, an arbitrary one is kept.

    Account ids are sourced from:
      - sso_account_id  (SSO profiles)
      - role_arn        (assume-role profiles)
    """
    config = configparser.ConfigParser()
    config.read(config_path)

    result: dict[str, str] = {}
    for section in config.sections():
        if not section.startswith("profile "):
            continue
        profile_name = section[len("profile ") :]
        account_id = config[section].get("sso_account_id")
        if not account_id:
            role_arn = config[section].get("role_arn", "")
            m = _ROLE_ARN_ACCOUNT_RE.search(role_arn)
            if m:
                account_id = m.group(1)
        if account_id:
            result[account_id] = profile_name

    return result


@lru_cache(maxsize=None)
def profile_to_account_id(config_path: Path) -> dict[str, str]:
    """Return the inverse mapping: profile name -> account id."""
    return {v: k for k, v in account_id_to_profile(config_path).items()}


def expand_account(account: str) -> str:
    """Resolve *account* to a 12-digit AWS account id.

    Passes through values that are already numeric account ids, the wildcard
    ``"*"``, or plain digits.  Otherwise the value is treated as an AWS config
    profile name and looked up in ``~/.aws/config``.  Raises ``ValueError`` if
    the profile name is not found.
    """
    if account.isdigit():
        return account
    mapping = profile_to_account_id(Path.home() / ".aws" / "config")
    account_id = mapping.get(account)
    if account_id is None:
        known = ", ".join(sorted(mapping))
        raise ValueError(
            f"Unknown account name '{account}'. " f"Known profile names: {known}"
        )
    return account_id


def expand_accounts(accounts: list[str]) -> list[str]:
    """Expand a list of account specifiers, resolving any profile names."""
    return [expand_account(a) for a in accounts]
