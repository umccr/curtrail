from typing import Tuple

import dateparser
import calendar
import re
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta

# written by Claude
# not exactly how I would have done it - but covers all basic cases we
# would have wanted - and took < 5 mins to make


def parse_date_range(user_input) -> Tuple[datetime, datetime]:
    """
    Parse user input into a date range tuple (start_date, end_date).
    Returns both as datetime objects with accurate period boundaries.
    """
    # Check if input contains range indicators
    range_separators = [" to ", " - ", "..", " until "]

    for sep in range_separators:
        if sep in user_input:
            parts = user_input.split(sep, 1)
            start = dateparser.parse(parts[0].strip())
            end = dateparser.parse(parts[1].strip())
            if start and end:
                # If end date is just a date (no time), set to end of day
                if end.hour == 0 and end.minute == 0 and end.second == 0:
                    end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
                return start, end

    lower = user_input.lower()
    now = datetime.now()

    # THIS MONTH
    if "this month" in lower:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day = calendar.monthrange(now.year, now.month)[1]
        end = now.replace(
            day=last_day, hour=23, minute=59, second=59, microsecond=999999
        )
        return start, end

    # THIS YEAR
    if "this year" in lower:
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(
            month=12, day=31, hour=23, minute=59, second=59, microsecond=999999
        )
        return start, end

    # THIS WEEK
    if "this week" in lower:
        # Get start of week (Monday)
        start = now - timedelta(days=now.weekday())
        # End of week (Sunday)
        end = start + timedelta(days=6)
        return start.today(), end.today()

    # RELATIVE DAYS (e.g., "last 7 days", "past 30 days") - check BEFORE dateparser
    if "last" in lower or "past" in lower:
        match = re.search(r"(\d+)\s*(day|week|month)", lower)
        if match:
            num = int(match.group(1))
            unit = match.group(2)

            end = datetime.now()
            if unit == "day":
                start = end - timedelta(days=num)
            elif unit == "week":
                start = end - timedelta(weeks=num)
            elif unit == "month":
                from dateutil.relativedelta import relativedelta

                start = end - relativedelta(months=num)

            return (start, end)

    # Check if input is just a year (YYYY)
    year_pattern = r"^\d{4}$"
    if re.match(year_pattern, user_input.strip()):
        year = int(user_input.strip())
        start = datetime(year, 1, 1, 0, 0, 0, 0)
        end = datetime(year, 12, 31, 23, 59, 59, 999999)
        return start, end

    # Check if input is a month specification (YYYY-MM or "Month YYYY" or "Month")
    month_year_pattern = r"^\d{4}-\d{2}$"  # 2025-01
    if re.match(month_year_pattern, user_input.strip()):
        # Parse as YYYY-MM
        date = dateparser.parse(user_input)
        if date:
            start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_day = calendar.monthrange(start.year, start.month)[1]
            end = start.replace(
                day=last_day, hour=23, minute=59, second=59, microsecond=999999
            )
            return start, end

    # Check for "Month YYYY" or just "Month" format
    month_names = "|".join(calendar.month_name[1:] + calendar.month_abbr[1:])
    month_pattern = rf"^({month_names})\s*(\d{{4}})?$"
    match = re.match(month_pattern, user_input.strip(), re.IGNORECASE)
    if match:
        date = dateparser.parse(user_input)
        if date:
            start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_day = calendar.monthrange(start.year, start.month)[1]
            end = start.replace(
                day=last_day, hour=23, minute=59, second=59, microsecond=999999
            )
            return start, end

    # Single date/period - interpret as a range
    date = dateparser.parse(user_input, settings={"PREFER_DATES_FROM": "past"})

    if date:
        # TODAY
        if "today" in lower or "now" in lower:
            return date.now(), date.now()

        # YESTERDAY
        if "yesterday" in lower:
            start = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
            return start, end

        # WEEK (last week, etc.)
        if "week" in lower:
            start = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=7) - timedelta(microseconds=1)
            return (start, end)

        # MONTH (last month, etc.)
        if "month" in lower:
            start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            last_day = calendar.monthrange(start.year, start.month)[1]
            end = start.replace(
                day=last_day, hour=23, minute=59, second=59, microsecond=999999
            )
            return (start, end)

        # YEAR (last year, etc.)
        if "year" in lower:
            start = date.replace(
                month=1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
            end = start.replace(
                month=12, day=31, hour=23, minute=59, second=59, microsecond=999999
            )
            return (start, end)

        # SPECIFIC DATE (default case)
        start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
        return (start, end)

    return None


def iter_months(start_date: datetime, end_date: datetime):
    """
    Generate tuples of (month_start, month_end) for each month in the range.
    Each tuple contains the actual start and end datetimes for that month
    within the given range.
    """
    # Start from the beginning of the first month
    current = start_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    while current <= end_date:
        # Calculate the last day of the current month
        last_day = calendar.monthrange(current.year, current.month)[1]
        month_end = current.replace(
            day=last_day, hour=23, minute=59, second=59, microsecond=999999
        )

        # Clamp to the actual range boundaries
        actual_start = max(current, start_date)
        actual_end = min(month_end, end_date)

        yield actual_start, actual_end

        # Move to the next month
        current = current + relativedelta(months=1)


# Test examples:
if __name__ == "__main__":
    test_cases = [
        "yesterday",
        "today",
        "this week",
        "this month",
        "this year",
        "last week",
        "last month",
        "last year",
        "2024",
        "2025",
        "January 2025",
        "Jan 2025",
        "2025-01",
        "December",
        "2025-01-15",
        "last 7 days",
        "past 30 days",
        "last 3 weeks",
        "past 6 months",
        "Jan 1 to Jan 15",
        "2025-01-01 - 2025-01-31",
    ]

    for case in test_cases:
        result = parse_date_range(case)
        print(f"{case:25} -> {result}")

    # Test with different ranges
    test_ranges = [
        parse_date_range("2024"),
        parse_date_range("January 2025 to March 2025"),
        parse_date_range("2024-06-15 to 2024-09-10"),
        parse_date_range("last 90 days"),
    ]

    for date_range in test_ranges:
        if date_range:
            start, end = date_range
            print(f"\nRange: {start} to {end}")
            print("Months:")
            for m_start, m_end in iter_months(start, end):
                print(
                    f"  {m_start.strftime('%Y-%m-%d')} to {m_end.strftime('%Y-%m-%d')}"
                )
