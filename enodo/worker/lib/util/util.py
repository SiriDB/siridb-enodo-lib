import datetime
import time


def get_dt_to_midnight():
    dt_obj = datetime.datetime.fromtimestamp(int(time.time()))
    return (dt_obj.replace(hour=0, minute=0, second=0) +
            datetime.timedelta(days=1)).timestamp()
