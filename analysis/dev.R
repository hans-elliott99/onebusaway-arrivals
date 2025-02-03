
library(data.table)

conn <- DBI::dbConnect(drv = RSQLite::SQLite(), dbname = "./scrape48.db")

dat <- DBI::dbGetQuery(conn = conn,
                       statement = "SELECT * FROM 'entries'")
setDT(dat)


dat <- dat[last_predicted_arrival != -1]
dat[, arr_gap_s := (last_predicted_arrival - scheduled_arrival)/1000]

mean(dat$arr_gap_s)
hist(dat$arr_gap_s)

