#!/usr/bin/env python3
#
# Tracking real time bus data from OneBusAway
# H. Elliott 2025
#

import os
import time
import datetime
import json
import asyncio
import sqlite3
import logging
# install:
import pytz
import polars as pl
from dotenv import load_dotenv
from onebusaway import OnebusawaySDK

load_dotenv()


if os.environ.get("LOCAL"):
    PATH_PREFIX = "./app/"
else:
    PATH_PREFIX = "./"



def current_service_date():
    dt = (
        datetime.datetime.now(datetime.timezone.utc)
        .astimezone(tz=pytz.timezone("US/Pacific"))
    )
    date = dt.date()
    if dt.hour <= 3:
        # if it's before 3am, service date is still the previous day
        date = date - datetime.timedelta(days=1)
    return date


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


def sysdt_to_logtime(t):
    return sysdt_to_pst(t, str_fmt='%Y-%m-%d  %H:%M:%S')


def _query_stop_schedule(client, stop_id, date):
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
            sysdt_to_pst,
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
    stop_schedule = stop_schedule.sort("arrival_time_str").with_columns(
        row_ix=pl.Series(range(stop_schedule.height))
    )
    return stop_schedule


def _query_arrival_time(client, stop_id, trip_id, service_date):
    """
    return (arrival_time, distance)
    """
    x = client.arrival_and_departure.retrieve(
        stop_id=stop_id,
        service_date=service_date,
        trip_id=trip_id
    )
    if x is None: # no data returned
        return (None, None)
    entry = json.loads(x.model_dump_json())["data"]["entry"]
    is_pred = entry["predicted"]
    pred = entry["predictedArrivalTime"]
    dist = entry["distanceFromStop"]
    if not is_pred:
        # not live yet, no prediction...
        pred = -1
    return (pred, dist)


class OBAClient:
    def __init__(self, api_key, logger, max_attempts=3, sleep_time=2):
        # note - could also use AsyncOnebusawaySDK, but not necessary yet
        self.api_key = api_key
        self.logger = logger
        self.max_attempts = max_attempts
        self.sleep_time = sleep_time
        self.total_fails = 0
        self.client = self.connect()
    
    def connect(self):
        ret = -2
        attempts = 0
        while attempts < self.max_attempts:
            attempts += 1
            try:
                # initalize and test connection
                client = OnebusawaySDK(api_key=self.api_key)
                ct = client.current_time.retrieve()
                ret = 0
            except Exception as e:
                self.logger.error(f"    Error connecting to OneBusAway: {e}")
                self.logger.info(f"    (Attempt {attempts} of {self.max_attempts})")
            if ret != -2:
                self.logger.info("Successfully connected to OneBusAway.")
                return client
            time.sleep(self.sleep_time)
        #
        raise Exception("Failed to connect to OneBusAway")
    
    def query_stop_schedule(self, stop_id, date):
        # if we've failed too many times, try to reconnect
        if self.total_fails > self.max_attempts * 5:
            self.logger.info("Attempting to reconnecting to OneBusAway...")
            self.client = self.connect()
            self.total_fails = 0
        out = None
        ret = -2
        attempts = 0
        while attempts < self.max_attempts:
            attempts += 1
            try:
                out = _query_stop_schedule(self.client, stop_id, date)
                ret = 0
            except Exception as e:
                self.logger.error(f"    Error querying stop schedule: {e}")
                self.logger.info(f"    (Attempt {attempts} of {self.max_attempts})")
                self.total_fails += 1
            if ret != -2:
                return out
            time.sleep(self.sleep_time)
        return out
    
    def query_arrival_time(self, stop_id, trip_id, service_date):
        # if we've failed too many times, try to reconnect
        if self.total_fails > self.max_attempts * 5:
            self.logger.info("Attempting to reconnecting to OneBusAway...")
            self.client = self.connect()
            self.total_fails = 0
        out = (None, None)
        ret = -2
        attempts = 0
        while attempts < self.max_attempts:
            attempts += 1
            try:
                out = _query_arrival_time(self.client, stop_id, trip_id, service_date)
                ret = 0
            except Exception as e:
                self.logger.error(f"    Error querying arrival time: {e}")
                self.logger.info(f"    (Attempt {attempts} of {self.max_attempts})")
                self.total_fails += 1
            if ret != -2:
                return out
            time.sleep(self.sleep_time)
        return out


class SqliteDB:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self._create_arrivals_table(self.conn)
    
    def _create_arrivals_table(self, conn):
        c = self.conn.cursor()
        c.execute(
        "CREATE TABLE IF NOT EXISTS arrivals " +
        "(stop_id TEXT, trip_id TEXT, service_date TEXT, schedule_id INTEGER, scheduled_arrival INTEGER, last_predicted_arrival INTEGER, distance_when_scraped REAL, time_when_scraped INTEGER)"
        )
        self.conn.commit()
        c.close()
    
    def insert_arrival(self,
                       stop_id,
                       trip_id,
                       service_date,
                       schedule_id,
                       scheduled_arrival,
                       last_predicted_arrival,
                       distance_when_scraped,
                       time_when_scraped):
        c = self.conn.cursor()
        c.execute("INSERT INTO arrivals VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                  (stop_id, trip_id, service_date, schedule_id, scheduled_arrival, last_predicted_arrival, distance_when_scraped, time_when_scraped))
        self.conn.commit()
        c.close()
    
    def close(self):
        self.conn.close()


def get_logger(filepath):
    logger = logging.getLogger("mainlog")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s:  %(message)s')

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(filepath)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger





#
# MAIN
#
# Plan:
# Start running the program.
# Each day it will need to collect the schedule for the stops of interest.
# It will use the scheduled arrival times to create an execution schedule:
#   It will plan to start checking in on each stop at the scheduled arrival time.
#   Once it checks:
#   - If the bus has not passed the stop yet (or still isn't live), check again
#     in X minutes (determine X based on distance from stop...current predicted arr time)
#     - At some point, if the bus is still not live, it probably won't come. Move on.
#   - If the bus has passed the stop, collect the last recorded predicted arrival time.
#     (It stops updating once the bus passes the stop, so this is the "final prediction".
#      But once the trip ends, we can no longer query this prediction.)
#
# For now, keep it simple and just scrape one stop.
#

class StopTripTaskParams:
    def __init__(self, stop_id, trip_id, service_date, scheduled_arrival, schedule_id):
        self.stop_id = stop_id
        self.trip_id = trip_id
        self.service_date = service_date
        self.scheduled_arrival = scheduled_arrival
        self.schedule_id = schedule_id


async def stoptrip_task(task_id: int,
                        client: OBAClient,
                        dbconn: SqliteDB,
                        logger: logging.Logger,
                        queue: asyncio.Queue):
    """
    This task checks the async queue for remaining stop-trip parameters - i.e.,
    stop-trip arrivals that still need to be tracked.
    If there are any, it will determine if it needs to wait longer before extracting
    the final predicted arrival time, or if it can extract the final predicted
    arrival time now.

    Note: we don't want to swarm the server with requests, and we really only
    need to check the predicted arrival time while the trip is live but *after*
    it has passed the stop (to get the final prediction).
    """
    while not queue.empty():
        params = await queue.get()
        stop_id = params.stop_id
        trip_id = params.trip_id
        schedule_id = params.schedule_id
        service_date = params.service_date
        scheduled_arrival = params.scheduled_arrival

        logger.info(f"[task {task_id}] stop: {stop_id}, trip: {trip_id}, scheduled: {sysdt_to_logtime(scheduled_arrival)}")

        now = time.time() * 1000
        arrival_gap = scheduled_arrival - now
        if arrival_gap/1000/60/60 < -1:
            # if the scheduled arrival time is more than an hour in the past
            # from now but we're still tracking this stop-trip, stop tracking 
            logger.info(f"    STOPPING - TRIP IS TOO OLD")
            continue
        elif arrival_gap/1000/60 > 5:
            # if the scheduled arrival time is more than 5 minutes in the future,
            # wait until it's closer to the scheduled arrival time before checking
            wait_secs = arrival_gap/1000
            logger.info(f"    WAITING {wait_secs :.2f}s - TOO EARLY TO CHECK")
            await asyncio.sleep(wait_secs)
            await queue.put(params)
            continue
        else:
            # check the scheduled arrival time...
            pr_arr_time, dist = client.query_arrival_time(
                stop_id=stop_id, trip_id=trip_id, service_date=service_date
            )
            scrape_time = time.time() * 1000
            if pr_arr_time is None:
                # query returned no data - usually due to invalid parameters
                logger.info(f"    STOPPING - ARRIVAL TIME QUERY RETURNED NO DATA")
                continue
            elif pr_arr_time == -1:
                # data received, but no predicted arrival time...
                if dist < 0:
                    # bus has already passed the stop but no prediction in data
                    # (ideally, shouldn't really happen)
                    logger.info(f"    TRIP PASSED BUT NO PREDICTION (dist={dist :.2f}m)")
                    dbconn.insert_arrival(
                        stop_id=stop_id, trip_id=trip_id, service_date=service_date,
                        schedule_id=schedule_id, scheduled_arrival=scheduled_arrival,
                        last_predicted_arrival=pr_arr_time, distance_when_scraped=dist,
                        time_when_scraped=scrape_time
                    )
                    continue
                else:
                    # bus not even live yet, check again closer to scheduled time 
                    wait_secs = arrival_gap/1000
                    logger.info(f"    WAITING {wait_secs :.2f}s - TRIP NOT YET LIVE")
                    await asyncio.sleep(wait_secs)
                    await queue.put(params)
                    continue
            elif dist >= 0:
                # bus not passed the stop yet, check again in a few minutes
                #   (based on the predicted arrival time)
                wait_secs = max((pr_arr_time - now)/1000 - 60, 60)
                logger.info(f"    WAITING {wait_secs :.2f}s - TRIP NOT YET PASSED ({dist :.2f} meters)")
                await asyncio.sleep(wait_secs)
                await queue.put(params)
                continue
            else:
                # bus has passed the stop and we have a prediction
                logger.info(f"    ARRIVED!! ARRIVAL: {sysdt_to_logtime(pr_arr_time)}")
                dbconn.insert_arrival(
                    stop_id=stop_id, trip_id=trip_id, service_date=service_date,
                    schedule_id=schedule_id, scheduled_arrival=scheduled_arrival,
                    last_predicted_arrival=pr_arr_time, distance_when_scraped=dist,
                    time_when_scraped=scrape_time
                )
                continue






async def daily_process(
        client: OBAClient,
        dbconn: SqliteDB,
        logger: logging.Logger,
        stop_id: str,
        current_date: str
    ):
    """
    Firt we query the stop schedule for the stop of interest.
    Then we loop through all scheduled arrivals at the stop and add their
      parameters (i.e., context needed for scraping api) to a work queue.
    Then we create a task for every scheduled arrival at the stop, so that
      there are guaranteed to be enough free tasks to process the work queue.
      - Each task determines if it needs to wait (based on the scheduled
        arrival time, and predicted arrival time once the trip is live), or if
        it can scrape the final predicted arrival time.
        Most tasks will spend a long time waiting. When they need to wait, we
        hang them up using asyncio.sleep() and then re-add their parameters to
        the work queue *after* the sleep so that a free task can pick up the work.
    
    Note, the documentation (https://developer.onebusaway.org/api/where/methods/arrival-and-departure-for-stop)
    says it is ideal to include vehicleId and stopSequence in your call to
    arrival-and-departure-for-stop, because sometimes multiple vehicles service
    the same trip and sometimes a single vehicle visits the same stop multiple
    times on one trip. However, we can't get that info from the stop schedule.
    I believe we'd need to fully monitor a vehicle to get that info.
    But this doesn't apply to my current stops of interest.
    """
    # load stop schedule for the current service date
    logger.info(f"** STOP ID: {stop_id} | SERVICE DATE: {current_date}")
    logger.info(f"** Querying stop schedule...")
    stop_schedule = client.query_stop_schedule(
        stop_id=stop_id,
        date=current_date
    )
    stop_schedule.write_parquet(PATH_PREFIX + f"stop_schedule/{stop_id}__{current_date}.parquet")

    logger.info("** Queueing up work and generating async tasks...")
    # add each stop-trip to the work queue to be processed
    work_queue = asyncio.Queue()
    for i in range(stop_schedule.height):
        trip = stop_schedule.filter(row_ix=i)
        params = StopTripTaskParams(
            stop_id=stop_id,
            trip_id=trip["trip_id"].item(),
            service_date=trip["service_date"].item(),
            scheduled_arrival=trip["arrival_time"].item(),
            schedule_id=i
        )
        await work_queue.put(params)
    
    # generate tasks to process the queue concurrently
    task_list = [
        asyncio.create_task(
            stoptrip_task(i, client, dbconn, logger, work_queue)
        ) for i in range(stop_schedule.height)
    ]

    # run the tasks
    if len(task_list) > 0:
        logger.info("** Running tasks...")
        await asyncio.gather(*task_list)
        logger.info("** Tasks all finished.")
    else:
        logger.info("** No tasks to run!")
    logger.info("**")





STOP_ID = "1_29278"   # 23rd & Republican
DBPATH  = PATH_PREFIX + "scrape.db"
LOGPATH = PATH_PREFIX + f"scrape_{datetime.datetime.now().date()}.log"
def main():
    logger = get_logger(LOGPATH)
    logger.setLevel(logging.INFO)
    logger.info("-------------------------------------------------")
    logger.info(f"Starting app.") 

    client = OBAClient(
        api_key=os.environ.get("ONEBUSAWAY_API_KEY"),
        logger=logger
    )
    dbconn = SqliteDB(DBPATH)

    current_date = current_service_date()
    previous_date = current_date - datetime.timedelta(days=1) # start with yesterday

    try:

        while True:
            logger.info("-------------------------------------------------iter")
            # check if it's a new day
            current_date = current_service_date()
            if current_date == previous_date:
                # This will only be reached if all relevant trips have ended
                # for the current service date BUT we haven't moved to the
                # next service date yet. In practice, that should only happen
                # late at night close to 3am, i.e., close to when the service
                # date changes.
                # In this case, we can just let the program sleep for a while
                # and then check back
                logger.info(f"* Same service date ({current_date}), taking a nap. Zzzzz...")
                time.sleep(60 * 10) # sleep for 10 minutes
            else:
                logger.info(f"* New service date detected: {current_date}")
                previous_date = current_date
                asyncio.run(
                    daily_process(
                        client=client,
                        dbconn=dbconn,
                        logger=logger,
                        stop_id=STOP_ID,
                        current_date=current_date
                    )
                )

    except (Exception, KeyboardInterrupt) as e:
        if isinstance(e, KeyboardInterrupt):
            logger.info("Keyboard interrupt received. Exiting app.")
        else:
            logger.error(f"Error: {e}")
        logger.info("Exiting app.")

        dbconn.close()
        client.client.close()
    

if __name__ == "__main__":
    main()
