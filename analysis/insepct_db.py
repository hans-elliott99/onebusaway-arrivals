import sqlite3
import pytz
import datetime
import time


def sysdt_to_pst(t, str_fmt='%Y-%m-%dT%H:%M:%SZ'):
    # The OneBusAway system reports all date-times in UTC, in milliseconds
    # since the epoch. This function converts that to a human-readable
    # date-time in Pacific Standard Time.
    t = datetime.datetime.fromtimestamp(
        # convert from ms to s after epoch, then to timestamp
        time.mktime(time.gmtime(t/1000))
    ).replace(tzinfo=pytz.utc).astimezone(tz=pytz.timezone("US/Pacific"))
    if str_fmt:
        t = datetime.datetime.strftime(t, format=str_fmt)
    return t


# query db for scraped arrivals
DB_PATH = "./app/data/data.db"
conn = sqlite3.connect("file:" + DB_PATH + "?mode=ro", uri=True) # read-only

c = conn.cursor()

arrivals = c.execute("SELECT * FROM arrivals")
col_names = [descr[0] for descr in c.description]
arrivals = arrivals.fetchall()

stp_ix = col_names.index("stop_id")
arr_ix = col_names.index("last_predicted_arrival")
sch_ix = col_names.index("scheduled_arrival")

# filter out dummy arrivals (app interruptions get logged as stop == -1)
arrivals = [arr for arr in arrivals if arr[stp_ix] != -1]
print(f"There are {len(arrivals)} arrivals in the database")

# get arrival times, and filter out arrivals with no predicted arrival time
arr_times = [sysdt_to_pst(arr[arr_ix]) for arr in arrivals if arr[arr_ix] != -1] ## filler for no obtained arrival time
print(f"There are {len(arr_times)} arrivals with scraped arrival times")

print(f"Earliest arrival: {min(arr_times)}")
print(f"Latest arrival:   {max(arr_times)}")

# arrivals per stop
bus_arr_count = {}
# and mean arrival gap per stop
bus_arr_gap = {}
for arr in arrivals:
    busstp = arr[stp_ix]
    if not busstp in bus_arr_count:
        bus_arr_count[busstp] = 0
        bus_arr_gap[busstp] = 0
    bus_arr_count[busstp] += 1
    bus_arr_gap[busstp] = (arr[sch_ix] - arr[arr_ix])/1000 

for busstp in bus_arr_count.keys():
    # convert to average
    bus_arr_gap[busstp] = bus_arr_gap[busstp] / bus_arr_count[busstp]

## sort bus dicts
bus_arr_count = {k: v for k, v in sorted(bus_arr_count.items(),
                                         key=lambda item: item[1],
                                         reverse=True)}
for k in bus_arr_gap.keys():
    print(f"Stop {k} - No. of Arrivals: {bus_arr_count[k]}, Mean seconds late: {bus_arr_gap[k] :.3f}")


conn.close()

