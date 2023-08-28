from datetime import datetime, timezone

UTC_Date_Format = "%Y-%m-%dT%H:%M:%SZ"


def utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_str() -> str:
    return utc().strftime(UTC_Date_Format)


def parse_utc_str(s: str) -> datetime:
    return datetime.strptime(s, UTC_Date_Format).replace(tzinfo=timezone.utc)
