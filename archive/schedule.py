
import polars as pl


class config:
    agency_id = 1


data_dir = "./kcm_schedule_files"

# agency = pl.read_csv(f"{data_dir}/agency.txt")
# Metro Transity agency_id = 1


# Get IDs for routes 43 and 48
routes = pl.read_csv(f"{data_dir}/routes.txt")
routes = routes.filter(
    (pl.col("route_short_name") == "48") | (pl.col("route_short_name") == "43")
).select(
    ["route_id", "route_short_name"]
)


# Get trips for routes 43 and 48
trips = pl.read_csv(f"{data_dir}/trips.txt")
trips = trips.filter(
    pl.col("route_id").is_in(routes["route_id"]) & (pl.col("direction_id") == 0)
)
## note - direction_id == 0 corresponds to outbound - ie, north towards campus


calendar = pl.read_csv(f"{data_dir}/calendar.txt")


# Get stop times for trips on routes 43 and 48 headed north
stops = pl.read_csv(f"{data_dir}/stops.txt")
stop_times = pl.read_csv(f"{data_dir}/stop_times.txt")

## filter down to stop of interest
stops = stops.filter(
    (pl.col("stop_name") == "23rd Ave E & E Republican St")
)
stop_times = stop_times.filter(
    # filter to this stop
    pl.col("stop_id").is_in(stops["stop_id"].unique())
    # filter to trips on 43/48 heading outbound
        & pl.col("trip_id").is_in(trips["trip_id"].unique())
)

## merge on route names
trip_to_name = trips.select(["trip_id", "route_id"]).unique().join(
    routes.select(["route_id", "route_short_name"]).unique(),
    on="route_id", how="left"
)
stop_times = stop_times.join(
    trip_to_name, on="trip_id", how="left"
)

## merge on calendar info
trip_to_calendar = trips.select(
    ["trip_id", "service_id"]
).unique().join(
    calendar, on="service_id", how="left"
)
stop_times = stop_times.join(
    trip_to_calendar, on="trip_id", how="left"
)

## convert arrival_time to type time
stop_times[["h", "m", "s"]] = stop_times["arrival_time"].str.split(":").list.to_struct()
stop_times = stop_times.with_columns(
    arr_time = pl.time(pl.col("h"), pl.col("m"), pl.col("s"))
)
stop_times = stop_times.drop(["h", "m", "s"])


## view my morning window
stop_times.filter(
    (pl.col("arr_time") > pl.time(7, 30, 0))
        & (pl.col("arr_time") < pl.time(10, 0, 0)) &
        (pl.col("monday") == 1)
).select(
    ["trip_id", "stop_id", "route_short_name", "arr_time", *calendar.columns]
).sort("arr_time").head(10)



