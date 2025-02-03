import os
import time
import pytz
import datetime
import json
import polars as pl
from onebusaway import OnebusawaySDK
from dotenv import load_dotenv
load_dotenv()


def systime_to_pst(t, str_fmt='%Y-%m-%dT%H:%M:%SZ'):
    t = datetime.datetime.fromtimestamp(
        # convert from ms to s after epoch, then to timestamp
        time.mktime(time.gmtime(t/1000))
    ).replace(tzinfo=pytz.utc).astimezone(tz=pytz.timezone("US/Pacific"))
    if str_fmt:
        t = datetime.datetime.strftime(t, format=str_fmt)
    return t


#
# query a specific date in a way that works consistently
# - and get all scheduled trips for the specific stop
#
# stop_id = "1_29278"
# date = "2025-01-21"
def query_stop_schedule(client, stop_id, date):
    sched = client.schedule_for_stop.retrieve(
        stop_id=stop_id,
        date=date
    )
    sched = json.loads(sched.model_dump_json())["data"]
    service_date = sched["entry"]["date"]

    # build stop schedule data set
    d = {"route_id": [],
         "service_date": [],
         "trip_id": [],
         "arrival_enabled": [],
         "arrival_time": [],
         "departure_enabled": [],
         "departure_time": [],
         "service_id": []}
    for entry in sched["entry"]["stop_route_schedules"]:
        for stop_times in entry["stop_route_direction_schedules"][0]["schedule_stop_times"]:
            d["route_id"].append( entry["route_id"] ) 
            d["service_date"].append( service_date )
            d["trip_id"].append( stop_times["trip_id"] )
            d["arrival_enabled"].append( stop_times["arrival_enabled"] )
            d["arrival_time"].append( stop_times["arrival_time"] )
            d["departure_enabled"].append( stop_times["departure_enabled"] )
            d["departure_time"].append( stop_times["departure_time"] )
            d["service_id"].append( stop_times["service_id"] )

    stop_schedule = pl.DataFrame(d)
    # convert arrival_time to usable format, PST
    stop_schedule = stop_schedule.with_columns(
        pl.col("arrival_time")
        .map_elements(
            systime_to_pst,
            return_dtype=pl.String
        ).alias(
            "arrival_time_str"
        )
    )
    stop_schedule = stop_schedule.with_columns(
        pl.col("arrival_time_str").str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ")
    )
    stop_schedule = stop_schedule.with_columns(
        pl.col("arrival_time_str").dt.date().alias("date").cast(pl.String),
        pl.col("arrival_time_str").dt.hour().alias("hour"),
        pl.col("arrival_time_str").dt.minute().alias("minute")
    )
    return stop_schedule


# https://pypi.org/project/onebusaway/
# 
client = OnebusawaySDK(
    api_key=os.environ.get("ONEBUSAWAY_API_KEY"),
)

# check connection
current_time = client.current_time.retrieve()
current_date = json.loads(
    current_time.model_dump_json()
)["data"]["entry"]["readable_time"].split("T")[0]

# # time, in ms since the unix epoch, of midnight for start of the service date for the trip
# t = time.time()
# service_date = int(t - t % 86400) # 86400 seconds in a day, so t % 86400 is the number of seconds since midnight
# # time.gmtime(service_date)
# service_date = service_date * 1000 # convert to ms for the API

# workflow:
#  * identify route of interest (e.g. 43, 48, 12)
#  * identify stop of interest for the given route (e.g. 23rd Ave E & E Republican St if route 48)
#  * identify trip of interest for the given route-stop on a specific date by
#    querying the schedule for the stop on the date of interest
#  * query the arrival/departure info for the trip at the stop
#    - if the trip is live (ie, currently running), we can get the predicted
#      arrival time, distance, etc.
#    - otherwise, we gain nothing that isn't in the predetermined schedule
#

routes = client.routes_for_agency.list(agency_id="1") # Metro Transit
routes = json.loads(routes.model_dump_json())["data"]["list"]
d = {"route_id": [], "route_short_name": [], "description": []}
for route in routes:
    d["route_id"].append(route["id"])
    d["route_short_name"].append(route["shortName"])
    d["description"].append(route["description"])
routes = pl.DataFrame(d)

route_id = routes.filter(
    pl.col("route_short_name") == "48"
).select("route_id").item()


stops = client.stops_for_route.list(route_id=route_id)
stops = json.loads(
    stops.model_dump_json()
)["data"]["references"]["stops"]
d = {"stop_id": [], "stop_name": [], "direction": []}
for stop in stops:
    d["stop_id"].append(stop["id"])
    d["stop_name"].append(stop["name"])
    d["direction"].append(stop["direction"])
stops = pl.DataFrame(d)

stops.filter(
    # pl.col("stop_id") == "1_13240"
    pl.col("stop_name").str.contains("23rd Ave E & E Republican St"),
    pl.col("direction") == "N"
)
stop_id = "1_13240" # can find via OneBusAway app too



stop_schedule = query_stop_schedule(client,
                                    stop_id=stop_id,
                                    date=current_date)

service_date = stop_schedule["service_date"][0]
stop_schedule.filter(
    pl.col("hour").is_between(18, 19)
).select(
    ["route_id", "trip_id", "arrival_time", "arrival_time_str"]
).sort("arrival_time_str")

# trip_id = "1_694183297"
trip_id = "1_694183537"

 
# at the specific stop on the specific date, get the arrival/departure info for the specific trip
# (so we would want to loop over all trips of interest running through this stop
#  on each date to get the full schedule)
x = client.arrival_and_departure.retrieve(
    stop_id=stop_id,
    service_date=service_date,
    trip_id=trip_id
)
if x is None:
    print("NO DATA")
arr = json.loads(x.model_dump_json())["data"]

entry = arr["entry"]     # actual data for the query
refs = arr["references"] # references/id-lookups for the data

entry["route_id"] # fixed for a given trip
entry["routeShortName"]
entry["trip_headsign"]

entry["scheduledArrivalTime"]
systime_to_pst(entry["scheduledArrivalTime"])

systime_to_pst(entry["scheduledDepartureTime"])

# unfortunately i think these all only apply to live trips, not historical data
# which means we need to scrape the data real-time to get the predictions...
#
entry["predicted"] # True = there is a predicted arrival/departure time, False = only scheduled times
systime_to_pst( entry["predictedArrivalTime"] ) # 0 if no prediction
entry["numberOfStopsAway"] # stops
entry["distanceFromStop"] # meters
entry["distanceFromStop"] * 0.000621371 # miles


## current_date = datetime.datetime.now().strftime("%Y-%m-%d")
## 
## # GET STOPs SCHEDULE FOR THE DAY
## stop_schedule = query_stop_schedule(client,
##                                     stop_id=config["stop_id"],
##                                     date=current_date)
## 
## trips_for_stop = stop_schedule.filter(
##     pl.col("hour").is_between(config["start_hour"], config["end_hour"])
## ).select(
##     ["route_id", "trip_id", "arrival_time", "arrival_time_str"]
## ).sort("arrival_time_str")
## 
## 
## # GET ID OF THE NEXT TRIP PASSING THROUGH THE STOP
## now = time.time() * 1000 # convert to ms to match data
## trip_id = trips_for_stop.filter(
##     pl.col("arrival_time") > now
## ).filter(
##     pl.col("arrival_time") == pl.col("arrival_time").min()
## ).select(
##     "trip_id"
## ).item()
## 
## 
## # QUERY ARRIVAL TIME
## stop_id = config["stop_id"]
## service_date = stop_schedule["service_date"][0]
## 
## arr_time, dist = query_arrival_time(client, stop_id, service_date, trip_id)
## if arr_time == -1:
##     print("NOT LIVE")
## elif dist > 0:
##     # if distance is positive, bus has not passed stop yet
##     print(f"DISTANCE: {abs(arr_time)}")
## else:
##     print(f"ARRIVAL: {systime_to_pst(arr_time)}")



# could do:
# 1. check trip 10 mins before scheduled arrival time
#   a. trip isn't live yet, wait another 5 mins/2.5/1.75/1
#   b. trip is live, extract predicted time, distance, stops away
#     aa. determine difference in current and predicted time to determine
#         when to check again.
#     bb. Continue checking trip regularly until it reaches stop. Consider precited
#         time as the "official arrival time" if.... (ideas?)
#           - check time/current time is within 1 minute of predicted time
#               - not good because the predicted arrival time can update a lot in
#                 the last minute 
#               - would want a combined check - current time is within 1 minute of
#                 predicted AND bus is 0 stops away
#               - or keep scraping time and storing them until the bus passes the stop,
#                then use the last time as the "official" arrival time
#           - bus is 1 or 0 stops away
#           - bus is only x meters away
#
#
# AH!
# As long as the trip is live, we can still get the final predicted time - the
# number of stops away and distance become negative, but the predicted time stops
# changing. So we just need to check a few times around the scheduled arrival time,
# wait for the bus to pass the stop, and then extract the final predicted time.
#
#
# So:
#   1. pre-select stops-on-routes of interest that we want to scrape
#      (e.g., 23rd Ave E & E Republican St for route 48)
#   2. generate the arrival/departure schedule for the stop on the date of interest
#      and filter down to all the trips in the time-window of interest
#      (e.g., keep all trips that arrive between 7:30 and 10:00)
#   3. now we have a list of trips and scheduled arrival times.
#      for each trip, start tracking the trip ~5 mins before the scheduled arrival time
#   2. generate the arrival/departure schedule for the stop on the date of interest
#      and filter down to all the trips in the time-window of interest
#      (e.g., keep all trips that arrive between 7:30 and 10:00)
#   3. now we have a list of trips and scheduled arrival times.
#      for each trip, start tracking the trip ~5 mins before the scheduled arrival time.
#   Trip tracking:
#   we just need to catch a trip while it's still live, but after it's gone past
#   the stop (so that the predicted arrival time is "final").
#  - Check the trip about 5 mins before the scheduled arrival time.
#    If the trip is not live when we first check, check again closer to the
#    scheduled arrival time.
#    If the trip is live, get the predicted arrival time and use it to determine
#    when to check again.
#    Continue like this until we check the trip and it has negative distance away
#    (it passed the stop) - then extract the predicted arrival time and stop
#    tracking.
#     
#     
