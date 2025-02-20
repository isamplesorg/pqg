"""
Test cases for some parts of pqg.common
"""
import datetime
import json
import zoneinfo
import pytest
import pqg.common


datetime_json_cases = (
    (
        datetime.datetime(2020, 1, 1, 12, 00,35, tzinfo=datetime.timezone.utc),
        '"2020-01-01T12:00:35+00:00"',
    ),
    (
        datetime.datetime(2020, 1, 1, 12, 00,35),
        '"2020-01-01T12:00:35+00:00"',
    ),
    (
        datetime.datetime(2020, 1, 1, 12, 00, 35, tzinfo=zoneinfo.ZoneInfo("America/Los_Angeles")),
        '"2020-01-01T12:00:35-08:00"',
    ),
)

@pytest.mark.parametrize("test, expected", datetime_json_cases)
def test_datetime_json_cases(test, expected):
    result = json.dumps(test, cls=pqg.common.JSONDateTimeEncoder)
    assert result == expected

