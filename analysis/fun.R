library(DBI)
library(data.table)
library(arrow)
library(ggplot2)


# database
db_path = "./data/backups/data_2025-06-11.db"
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

arrivals[, `:=`(
    dow = data.table::wday(service_date_str),
    month = data.table::month(service_date_str),
    hour = data.table::hour(scheduled_arrival_str)
)]


## you can also merge arrival data onto the daily schedule:
schedule <- merge(schedule,
                  arrivals[, .(service_date, stop_id, route_id, trip_id, tracked = 1)],
                  by = c("service_date", "stop_id", "route_id", "trip_id"),
                  all.x = TRUE)

mean(arrivals$arr_gap_s)
hist(arrivals$arr_gap_s)

ggplot(arrivals, aes(x = arr_gap_s/60)) +
    geom_histogram() +
    facet_wrap(~stop_name)


# overall arrival gap by route at a specific stop
arrivals[stop_name == "23rd Ave E & E Republican St" &
             route_short != "988"] |>
    ggplot(aes(x = arr_gap_s/60, color = route_short)) +
    geom_density()


# arrival gap on a given day by route at a specific stop, faceted by hour
arrivals[stop_name == "23rd Ave E & E Republican St" &
             route_short != "988" &
             wday(service_date_str) == 4] |>
    ggplot(aes(x = arr_gap_s/60, color = route_short)) +
    geom_density() +
    facet_wrap(~hour(scheduled_arrival_str)) +
    geom_vline(xintercept = 0, linetype = "dashed", alpha = 0.5)


x <- arrivals[between(arr_gap_s, -5 * 60, 10 * 60) &
                  route_short != "988"]
x[, wday := wday(service_date_str)]

x_med <- x[, .(arr_gap_s = median(arr_gap_s, na.rm = TRUE)),
           by = .(stop_name, route_short, wday)]

ggplot(x, aes(x = wday,
           y = arr_gap_s/60,
           group = wday)) +
    geom_jitter(alpha = 0.2, width = 0.2, height = 0, size = 1,
                color = "#f69b01") +
    geom_boxplot(fill = NA, outliers = FALSE, color = "#2c7084") +
    geom_text(
        data = x_med,
        aes(label = round(arr_gap_s/60, 1)),
        vjust = -0.3, size = 4,
        color = "black"
    ) +
    scale_x_continuous(
        breaks = 1:7,
        labels = c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    ) +
    facet_wrap(~stop_name + route_short) +
    labs(
        title = "King County Metro: Arrival Gap by Day of Week",
        caption = "Constrained to arrivals within 5 minutes early and 10 minutes late of schedule (97.5% of all arrivals)",
        x = "Day of Week",
        y = "Arrival Gap (minutes)"
    ) +
    theme_minimal(16)

ggsave("./images/arrival_gap_by_dow.png",
       width = 12, height = 8, dpi = 300)
