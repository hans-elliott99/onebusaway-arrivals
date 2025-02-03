
library(data.table)
library(arrow)

conn <- DBI::dbConnect(drv = RSQLite::SQLite(),
                       dbname = "./app/scrape_2025-02-02.db")

dat <- DBI::dbGetQuery(conn = conn,
                       statement = "SELECT * FROM 'arrivals'")
setDT(dat)

DBI::dbDisConnect(conn)


dat <- dat[last_predicted_arrival != -1]
dat[, arr_gap_s := (last_predicted_arrival - scheduled_arrival)/1000]

mean(dat$arr_gap_s)
hist(dat$arr_gap_s)



# features
sched <- arrow::read_parquet("./app/stop_schedule/2025-02-02.parquet")
setDT(sched)

stops <- arrow::read_parquet("./data/stops.parquet")
setDT(stops)

routes <- arrow::read_parquet("./data/routes.parquet")
setDT(routes)




