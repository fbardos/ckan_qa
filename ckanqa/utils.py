import datetime as dt
import math
import os
from typing import List


def generate_date_range(
    start: dt.datetime = dt.datetime(dt.date.today().year, 1, 1, tzinfo=dt.timezone.utc),
    end: dt.datetime = dt.datetime.combine(dt.date.today(), dt.datetime.min.time(), tzinfo=dt.timezone.utc) - dt.timedelta(days=1),
    interval_min: int = 60*24,

) -> List[dt.datetime]:
    delta_s = (end - start).total_seconds()
    hops = math.floor(delta_s / interval_min / 60) + 1
    return [start + dt.timedelta(minutes=interval_min * i) for i in range(hops)]


# Source https://stackoverflow.com/a/14819803
def mkdir_p(sftp, remote_directory):
    """Change to this directory, recursively making new folders if needed.
    Returns True if any folders were created."""
    if remote_directory == '/':
        # absolute path so change directory to root
        sftp.chdir('/')
        return
    if remote_directory == '':
        # top-level relative directory must exist
        return
    try:
        sftp.chdir(remote_directory) # sub-directory exists
    except IOError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        mkdir_p(sftp, dirname) # make parent directories
        sftp.mkdir(basename) # sub-directory missing, so created it
        sftp.chdir(basename)
        return True
