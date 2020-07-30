# Title: Load Load sites.csv data, merge
# Author: Karlo Lukic, karlo.lukic@pm.me
# Date: 19.04.20
# Description:
# This code loads WhoTracks.me historical 'sites.csv's data for global traffic
# and merges it.

# libs
library(data.table)
library(dplyr)
library(usethis)
library(stringr)

# console output options
options(digits = 3, max.print = 1000000)


# load data ----
# load all monthly global 'sites.csv' datasets from: https://t.ly/pjhm
# 2017 (start: May 2017)
ui_todo("\n loading historical global WhoTracks.me datasets from GitHub")
may_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-05/global/sites.csv")
jun_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-06/global/sites.csv")
jul_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-07/global/sites.csv")
aug_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-08/global/sites.csv")
sep_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-09/global/sites.csv")
oct_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-10/global/sites.csv")
nov_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-11/global/sites.csv")
dec_2017_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-12/global/sites.csv")
# 2018
jan_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-01/global/sites.csv")
feb_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-02/global/sites.csv")
mar_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-03/global/sites.csv")
apr_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-04/global/sites.csv")
may_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-05/global/sites.csv")
jun_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-06/global/sites.csv")
jul_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-07/global/sites.csv")
aug_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-08/global/sites.csv")
sep_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-09/global/sites.csv")
oct_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-10/global/sites.csv")
nov_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-11/global/sites.csv")
dec_2018_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-12/global/sites.csv")
# 2019
jan_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-01/global/sites.csv")
feb_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-02/global/sites.csv")
mar_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-03/global/sites.csv")
apr_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-04/global/sites.csv")
may_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-05/global/sites.csv")
jun_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-06/global/sites.csv")
jul_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-07/global/sites.csv")
aug_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-08/global/sites.csv")
sep_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-09/global/sites.csv")
oct_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-10/global/sites.csv")
nov_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-11/global/sites.csv")
dec_2019_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-12/global/sites.csv")
# 2020
# TODO: add new release
jan_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-01/global/sites.csv")
feb_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-02/global/sites.csv")
mar_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-03/global/sites.csv",
  drop = "V1" # V1 var = row ID
)
ui_done("\n historical global WhoTracks.me datasets downlaoded from GitHub")


# combine by year ----
sites_2017 <-
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
sites_2018 <-
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
sites_2019 <-
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
sites_2020 <-
  rbind(
    jan_2020_DT,
    feb_2020_DT,
    mar_2020_DT, # 2 added vars: t_active, cookie_samesite_none
    fill = T
  )


# structures
dim(sites_2017) # 3 cols missing
dim(sites_2018)
dim(sites_2019)
dim(sites_2020)

# merge datasets ----
sites_DT <- rbind(sites_2017,
  sites_2018,
  sites_2019,
  sites_2020,
  fill = T # needed for 3 extra 'referrer' cols (measure started in Aug 2018)
)
dim(sites_DT)
sites_DT
glimpse(sites_DT)

# unique N sites visited over the years?
uniqueN(sites_DT, by = "site")

# unique categories visited over the years?
uniqueN(sites_DT, by = "category")


# recode month to date ----
# mark last day of each month, thanks to: https://bit.ly/3eDGvZD
fom <- function(x) as.Date(cut(x, "month"))
# 1st step: to full date
sites_DT[, month := as.Date(paste(month, 01, sep = "-"))][, .(month)]
# 2nd step: to last day of each month
sites_DT[, month := fom(month + 32) - 1][, .(month)]

# reorder month as starting var
setcolorder(sites_DT, "month")
glimpse(sites_DT)

# fix 'Governmental' sites ----
sites_DT[site == "nih.gov", .(site, category)] # problematic site example
problematic_sites <- as.character(sites_DT[category == "Government", unique(site)])
subset_DT <- sites_DT[site %in% problematic_sites, .(site, category)]
unique(subset_DT$category)
subset_DT[, category := str_replace(category, "Reference", "Government")]
# subset_DT[, category := stringr::str_replace(category, "Business", "Government")]
# subset_DT[, category := stringr::str_replace(category, "News and Portals", "Government")]
unique(subset_DT$category)
sites_DT[site %in% problematic_sites, c("site", "category")] <- subset_DT
sites_DT[site == "nih.gov", .(site, category)] # problematic site
unique(sites_DT$category)


# recode chars/numerics to factors ----
# 1. country
# 2. site
# 3. category
vars <- c(
  "country", "site", "category"
)
sites_DT[, lapply(.SD, class), .SDcols = vars]
sites_DT[, (vars) := lapply(.SD, as.factor), .SDcols = vars]
sites_DT[, lapply(.SD, class), .SDcols = vars]
glimpse(sites_DT)

# number of sites visited by global users accross time?
sites_DT[, uniqueN(site)]

# number of sites in each category?
sites_DT[, .(n = uniqueN(site)), by = .(category)][order(-n)]

# save unique 447 websites (panel data)
panel_sites <- sites_DT[, .(site = unique(site))]


# export ----
# as .rds to 'global' folder in 'data/sample'
dir.create(file.path("./data/global"), showWarnings = F)
saveRDS(sites_DT, file = "./data/global/sites_DT.rds", compress = F)

# check
list.files("./data/sample/global")

# test loading: correct
rm(list = ls())
sites_DT <- readRDS("./data/global/sites_DT.rds")
sites_DT
glimpse(sites_DT)
summary(sites_DT)
