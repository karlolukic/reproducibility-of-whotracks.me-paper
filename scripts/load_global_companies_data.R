# Title: Load global companies.csv data, merge
# Author: Karlo Lukic
# Date: 19.04.20
# Description: 
# This code loads WhoTracks.me historical 'companies.csv' data for global traffic and merges it


# libs
library(data.table)
library(RSQLite)
library(DBI)


# global options
options(max.print = 1000000)


# load data ----
# load all monthly global 'companies.csv' datasets from: https://t.ly/pjhm
# 2017 (start: May 2017)
usethis::ui_todo('\n loading all historical global who.tracks.me datasets from GitHub')
may_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-05/global/companies.csv')
jun_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-06/global/companies.csv')
jul_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-07/global/companies.csv')
aug_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-08/global/companies.csv')
sep_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-09/global/companies.csv')
oct_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-10/global/companies.csv')
nov_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-11/global/companies.csv')
dec_2017_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2017-12/global/companies.csv')
# 2018
jan_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-01/global/companies.csv')
feb_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-02/global/companies.csv')
mar_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-03/global/companies.csv')
apr_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-04/global/companies.csv')
may_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-05/global/companies.csv')
jun_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-06/global/companies.csv')
jul_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-07/global/companies.csv')
aug_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-08/global/companies.csv')
sep_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-09/global/companies.csv')
oct_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-10/global/companies.csv')
nov_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-11/global/companies.csv')
dec_2018_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2018-12/global/companies.csv')
# 2019
jan_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-01/global/companies.csv')
feb_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-02/global/companies.csv')
mar_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-03/global/companies.csv')
apr_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-04/global/companies.csv')
may_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-05/global/companies.csv')
jun_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-06/global/companies.csv')
jul_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-07/global/companies.csv')
aug_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-08/global/companies.csv')
sep_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-09/global/companies.csv')
oct_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-10/global/companies.csv')
nov_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-11/global/companies.csv')
dec_2019_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2019-12/global/companies.csv')
# 2020
# TODO: add new release
jan_2020_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-01/global/companies.csv')
feb_2020_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-02/global/companies.csv')
mar_2020_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-03/global/companies.csv',
                     drop = 'V1') # V1 var = row ID
usethis::ui_done('\n all historical global who.tracks.me datasets downlaoded from GitHub')

# combine by year ----
companies_2017 <-
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
companies_2018 <-
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
    fill=T # needed for 3 extra 'referrer' cols (measure started in Aug 2018)
  )
companies_2019 <- 
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
companies_2020 <- 
  rbind(
    jan_2020_DT,
    feb_2020_DT,
    mar_2020_DT, # 2 added vars: t_active, cookie_samesite_none
    fill=T 
  )


# structures
dim(companies_2017) # 3 cols missing
dim(companies_2018)
dim(companies_2019)
dim(companies_2020)

# merge datasets ----
companies_DT <- rbind(companies_2017,
                  companies_2018,
                  companies_2019,
                  companies_2020,
                  fill=T # needed for extra cols
                  )
dim(companies_DT)
companies_DT
dplyr::glimpse(companies_DT)

# unique N of companies over the years?
uniqueN(companies_DT, by='company')

# recode month to date ----
# mark last day of each month, thanks to: https://bit.ly/3eDGvZD
fom <- function(x) as.Date(cut(x, "month"))
# 1st step: to full date
companies_DT[, month := as.Date(paste(month, 01, sep = "-"))]
# 2nd step: to last day of each month
companies_DT[, month := fom(month + 32) - 1]
# reorder month as starting var
dplyr::glimpse(companies_DT)


# add clean company names
# use trackerdb.sqlite file
# connect to db: con
con <- dbConnect(SQLite(), './data/tracker_database/trackerdb.sqlite')
dbListTables(con)
dbListFields(con, "companies")

# read-in, to data.table: trackers_sql, categories_sql
companies_sql <- setDT(dbReadTable(con, "companies")) # linked to 'id'
dbDisconnect(con)
companies_sql <- companies_sql[, c('id', 'name', 'country')] # remove cols
old_names <- c('id', 'name', 'country')
new_names <- c('company', 'company_name', 'org_country')
setnames(companies_sql, old_names, new_names)
head(companies_sql)

# add to companies_DT
companies_DT <- companies_sql[companies_DT, on = 'company']
companies_DT

# set 'month' and "country" as first vars
setcolorder(companies_DT, c('month', 'country'))

# recode numerics to factors ----
# 1. country
# 2. company
# 3. company_name
# 4. org_country
vars <- c('country', 'company', 'company_name', 'org_country')
companies_DT[, lapply(.SD, class), .SDcols = vars]
companies_DT[, (vars) := lapply(.SD, as.factor), .SDcols = vars]
companies_DT[, lapply(.SD, class), .SDcols = vars]

# fix 'org_country'
summary(companies_DT$org_country)
companies_DT[, org_country := as.factor(tolower(org_country))]
summary(companies_DT$org_country)

# recode numerics to integers ----
# 1. companies
class(companies_DT$companies)
companies_DT[, companies := as.integer(companies)]
class(companies_DT$companies)





# export ----
# as .rds to 'global' folder in 'data'
dir.create(file.path('./data/global'), showWarnings = F)
saveRDS(companies_DT, file='./data/global/companies_DT.rds', compress = F)
list.files('./data/global')

# test loading: correct
rm(list=ls())
companies_DT <- readRDS('./data/global/companies_DT.rds')
companies_DT
dplyr::glimpse(companies_DT)
summary(companies_DT)


