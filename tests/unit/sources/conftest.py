import pytest
from pyspark.sql import Row
from datetime import datetime


@pytest.fixture
def expected_result():
    return [
        Row(id=1, data="abc", date=datetime(2021, 1, 1).date()),
        Row(id=2, data="def", date=datetime(2021, 1, 2).date()),
    ]
