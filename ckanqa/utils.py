import datetime as dt
import math
from typing import List


def generate_date_range(
    start: dt.datetime = dt.datetime(dt.date.today().year, 1, 1, tzinfo=dt.timezone.utc),
    end: dt.datetime = dt.datetime.combine(dt.date.today(), dt.datetime.min.time(), tzinfo=dt.timezone.utc) - dt.timedelta(days=1),
    interval_min: int = 60*24,

) -> List[dt.datetime]:
    delta_s = (end - start).total_seconds()
    hops = math.floor(delta_s / interval_min / 60) + 1
    return [start + dt.timedelta(minutes=interval_min * i) for i in range(hops)]


