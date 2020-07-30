# Title: Load trackers.csv data, merge
# Author: Karlo Lukic, karlo.lukic@pm.me
# Date: 19.04.20
# Description:
# This code loads WhoTracks.me historical 'trackers.csv' data for global
# traffic, merges it, and adds additional tracker info (category, mother
# company, etc.).


# libs
library(data.table)
library(usethis)
library(dplyr)
library(stringr)

# global options
options(max.print = 1000000)


# load data ----
# load all monthly global 'trackers.csv' datasets from: https://t.ly/pjhm
# 2017 (start: May 2017)
ui_todo("\n loading historical global WhoTracks.me datasets from GitHub")
may_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-05/global/trackers.csv")
jun_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-06/global/trackers.csv")
jul_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-07/global/trackers.csv")
aug_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-08/global/trackers.csv")
sep_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-09/global/trackers.csv")
oct_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-10/global/trackers.csv")
nov_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-11/global/trackers.csv")
dec_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-12/global/trackers.csv")
# 2018
jan_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-01/global/trackers.csv")
feb_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-02/global/trackers.csv")
mar_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-03/global/trackers.csv")
apr_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-04/global/trackers.csv")
may_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-05/global/trackers.csv")
jun_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-06/global/trackers.csv")
jul_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-07/global/trackers.csv")
aug_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-08/global/trackers.csv")
sep_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-09/global/trackers.csv")
oct_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-10/global/trackers.csv")
nov_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-11/global/trackers.csv")
dec_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-12/global/trackers.csv")
# 2019
jan_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-01/global/trackers.csv")
feb_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-02/global/trackers.csv")
mar_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-03/global/trackers.csv")
apr_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-04/global/trackers.csv")
may_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-05/global/trackers.csv")
jun_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-06/global/trackers.csv")
jul_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-07/global/trackers.csv")
aug_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-08/global/trackers.csv")
sep_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-09/global/trackers.csv")
oct_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-10/global/trackers.csv")
nov_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-11/global/trackers.csv")
dec_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-12/global/trackers.csv")
# 2020
# TODO: add new release
jan_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-01/global/trackers.csv")
feb_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-02/global/trackers.csv")
mar_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-03/global/trackers.csv",
  drop = "V1"
) # V1 var = row ID
ui_done("\n historical global WhoTracks.me datasets downlaoded from GitHub")

# combine by year ----
trackers_2017 <-
  rbind(
    may_2017_DT,
    jun_2017_DT,
    jul_2017_DT,
    aug_2017_DT,
    sep_2017_DT,
    oct_2017_DT,
    nov_2017_DT,
    dec_2017_DT
  )
trackers_2018 <-
  rbind(
    jan_2018_DT,
    feb_2018_DT,
    mar_2018_DT,
    apr_2018_DT,
    may_2018_DT,
    jun_2018_DT,
    jul_2018_DT,
    aug_2018_DT,
    sep_2018_DT,
    oct_2018_DT,
    nov_2018_DT,
    dec_2018_DT,
    fill = T # needed for 3 extra 'referrer' cols (measure started in Aug 2018)
  )
trackers_2019 <-
  rbind(
    jan_2019_DT,
    feb_2019_DT,
    mar_2019_DT,
    apr_2019_DT,
    may_2019_DT,
    jun_2019_DT,
    jul_2019_DT,
    aug_2019_DT,
    sep_2019_DT,
    oct_2019_DT,
    nov_2019_DT,
    dec_2019_DT
  )
# TODO: add new release
trackers_2020 <-
  rbind(
    jan_2020_DT,
    feb_2020_DT,
    mar_2020_DT, # 2 added vars: t_active, cookie_samesite_none
    fill = T
  )


# structures
dim(trackers_2017) # 3 cols missing
dim(trackers_2018)
dim(trackers_2019)
dim(trackers_2020)

# merge datasets ----
trackers_DT <- rbind(trackers_2017,
  trackers_2018,
  trackers_2019,
  trackers_2020,
  fill = T # needed for 3 extra 'referrer' cols (measure started in Aug 2018)
)
dim(trackers_DT)
trackers_DT
glimpse(trackers_DT)

# unique N of trackers over the years?
uniqueN(trackers_DT, by = "tracker")


# recode month to date ----
# mark last day of each month, thanks to: https://bit.ly/3eDGvZD
fom <- function(x) as.Date(cut(x, "month"))
# 1st step: to full date
trackers_DT[, month := as.Date(paste(month, 01, sep = "-"))]
# 2nd step: to last day of each month
trackers_DT[, month := fom(month + 32) - 1]
glimpse(trackers_DT)


# add tracker categories ----
# use trackerdb.sqlite file
# connect to db: con
con <- dbConnect(SQLite(), "./data/tracker_database/trackerdb.sqlite")
dbListTables(con)
dbListFields(con, "trackers") # linked via 'category_id'
dbListFields(con, "categories") # linked to 'id'

# read-in, to data.table: trackers_sql, categories_sql
trackers_sql <- setDT(dbReadTable(con, "trackers"))
categories_sql <- setDT(dbReadTable(con, "categories")) # linked to 'id'
dbDisconnect(con)

# total of 13 tracker categories (12 in reality)
categories_sql

# remove unnecessary cols
head(trackers_sql)
trackers_sql <- trackers_sql[, .(id, name, category_id, company_id)]
head(trackers_sql)

# add tracker category to trackers
# join sql tables on 'id' and 'category_id'
trackers_sql <- trackers_sql[categories_sql, on = .(category_id = id)] # expand
old_names <- c("id", "name", "company_id", "i.name")
new_names <- c("tracker", "tracker_name", "company", "tracker_category")
setnames(trackers_sql, old_names, new_names) # rename
trackers_sql[, category_id := NULL] # drop
trackers_sql

# add tracker category, formatted name and mother company to trackers_DT
trackers_DT <- trackers_sql[trackers_DT, on = "tracker"]
trackers_DT

# move 'month' to start
setcolorder(trackers_DT, "month")
trackers_DT


# fix NAs ----
# in tracker_name
# 'NA' to "unknown"
sum(is.na((trackers_DT$tracker_name)))
trackers_DT[, tracker_name := replace(tracker_name, is.na(tracker_name), "unknown")]
sum(is.na((trackers_DT$tracker_name)))

# in company
# 'NA' to "unknown"
sum(is.na(trackers_DT$company))
trackers_DT[, company := replace(company, is.na(company), "unknown")]
sum(is.na(trackers_DT$company))

# in tracker_category
# 'NA' to "unknown"
sum(is.na(trackers_DT$tracker_category))
trackers_DT[, tracker_category := replace(tracker_category, is.na(tracker_category), "unknown")]
sum(is.na(trackers_DT$tracker_category))

# rename tracker categories ----
unique(trackers_DT$tracker_category)
trackers_DT[, tracker_category := str_replace(tracker_category, "site_analytics", "Site analytics")]
trackers_DT[, tracker_category := str_replace(tracker_category, "advertising", "Advertising")]
trackers_DT[, tracker_category := str_replace(tracker_category, "essential", "Essential")]
trackers_DT[, tracker_category := str_replace(tracker_category, "cdn", "CDN")]
trackers_DT[, tracker_category := str_replace(tracker_category, "hosting", "Hosting")]
trackers_DT[, tracker_category := str_replace(tracker_category, "social_media", "Social media")]
trackers_DT[, tracker_category := str_replace(tracker_category, "audio_video_player", "Audio video player")]
trackers_DT[, tracker_category := str_replace(tracker_category, "extensions", "Extensions")]
trackers_DT[, tracker_category := str_replace(tracker_category, "customer_interaction", "Customer interaction")]
trackers_DT[, tracker_category := str_replace(tracker_category, "pornvertising", "Adult advertising")]
trackers_DT[, tracker_category := str_replace(tracker_category, "misc", "Misc")]
trackers_DT[, tracker_category := str_replace(tracker_category, "unknown", "Unknown")]
trackers_DT[, tracker_category := str_replace(tracker_category, "comments", "Comments")]


# recode ----
# chars/numerics to factors
# 1. country
# 2. tracker
# 3. tracker_name
# 4. company
# 5. tracker_category
vars <- c("country", "tracker", "tracker_name", "company", "tracker_category")
trackers_DT[, lapply(.SD, class), .SDcols = vars]
trackers_DT[, (vars) := lapply(.SD, as.factor), .SDcols = vars]
trackers_DT[, lapply(.SD, class), .SDcols = vars]
glimpse(trackers_DT)


# numerics to integers
# 1. trackers
# 2. companies
vars <- c("trackers", "companies")
trackers_DT[, lapply(.SD, class), .SDcols = vars]
trackers_DT[, (vars) := lapply(.SD, as.integer), .SDcols = vars]
trackers_DT[, lapply(.SD, class), .SDcols = vars]
glimpse(trackers_DT)



# export ----
# as .rds to 'global' folder in 'data'
dir.create(file.path("./data/global"), showWarnings = F)
saveRDS(trackers_DT, file = "./data/global/trackers_DT.rds", compress = F)
list.files("./data/global")

# test loading: correct
rm(list = ls())
trackers_DT <- readRDS("./data/global/trackers_DT.rds")
trackers_DT
glimpse(trackers_DT)
summary(trackers_DT)