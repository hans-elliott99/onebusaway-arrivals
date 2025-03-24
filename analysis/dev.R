
library(data.table)
library(arrow)
library(ggplot2)

# database
db_path = "./data/backups/data_2025-02-04.db"
# db_path = "./app/data/data.db"
conn <- DBI::dbConnect(drv = RSQLite::SQLite(),
                       dbname = db_path)
arrivals <- DBI::dbGetQuery(conn = conn,
                            statement = "SELECT * FROM 'arrivals'")
schedule <- DBI::dbGetQuery(conn = conn,
                            statement = "SELECT * FROM 'stop_schedule'")
setDT(arrivals); setDT(schedule)
DBI::dbDisconnect(conn)

# features
stops <- arrow::read_parquet("./data/stops.parquet")
setDT(stops)
routes <- arrow::read_parquet("./data/routes.parquet")
setDT(routes)


arrivals <- arrivals[last_predicted_arrival != -1] ## bus arrived but no arrival time reported
arrivals <- arrivals[stop_id != -1] ## error or app interruption
arrivals[, arr_gap_s := (last_predicted_arrival - scheduled_arrival)/1000]

arrivals[, `:=` (
    service_date_str = as.POSIXct(service_date/1000, origin="1970-01-01"),
    scheduled_arrival_str = as.POSIXct(scheduled_arrival/1000, origin="1970-01-01"),
    predicted_arrival_str = as.POSIXct(last_predicted_arrival/1000, origin="1970-01-01"),
    time_scraped_str = as.POSIXct(time_when_scraped/1000, origin="1970-01-01")
)]
arrivals <- merge(arrivals,
                  stops[, .(stop_id, stop_name = name)],
                  by = "stop_id", all.x = TRUE)
arrivals <- merge(arrivals,
                  routes[, .(route_id, route_short = short_name, route_long = description)],
                  by = "route_id", all.x = TRUE)
## you can also merge with the daily schedule:
schedule <- merge(schedule,
                  arrivals[, .(service_date, stop_id, route_id, trip_id, tracked = 1)],
                  by = c("service_date", "stop_id", "route_id", "trip_id"),
                  all.x = TRUE)

mean(dat$arr_gap_s)
hist(dat$arr_gap_s)

ggplot(dat, aes(x = arr_gap_s)) +
    geom_histogram() +
    facet_wrap(~stop_id)

