# Title:
# Load sites.csv data, merge, label groups
# Author:
# Karlo Lukic
# Date:
# 28.05.20
# Description:
# This code loads WhoTracks.me historical 'sites.csv's data for global traffic, finds
# observations that are unique over time, merges them creating a panel dataset,
# labels control/treatment groups according to server location & ccTLD,
# adds 'time_dummy' indicator of GDPR implementation in May 2018,
# adds post-treatment time indicators for long-term GDPR's effects,
# adds pre-treatment indicator for parallel trends' assumption test, and
# recodes variables to analysis-friendly format.

# libs
library(data.table)
library(dplyr)
library(curl)
library(usethis)
library(rgeolocate)
library(stringr)
library(urltools)

# console output options
options(digits = 3, max.print = 1000000)


# load data ----
# load all monthly global 'sites.csv' datasets from: https://t.ly/pjhm
# 2017 (start: May 2017)
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
apr_2020_DT <- fread("https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-04/global/sites.csv",
  drop = "V1"
)
may_2020_DT <- fread('https://media.githubusercontent.com/media/cliqz-oss/whotracks.me/master/whotracksme/data/assets/2020-05/global/sites.csv',
                      drop = 'V1')


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
    apr_2020_DT, 
    may_2020_DT,
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



# track unique sites over time ----
# find unique sites present over time
sites_DT[, .(unique_sites = uniqueN(site)), by = .(month)] # number increases over time
sites_DT[site == "01net.com", .(site, month)] # example that's constant over time
sites_DT[site == "zvab.com", .(site, month)] # example that needs to be filtered out

# write function: filter_sites
filter_sites <- function(DT) {
  # find unique N of sites by month: sites
  sites <- DT[, .(unique_sites = uniqueN(site)), by = .(month)]
  # find min. N of above sites: min_unique_sites
  min_unique_sites <- min(sites$unique_sites)
  # find corresponding month where the min is: min_unique_sites_month
  min_unique_sites_month <- sites[unique_sites %in% min_unique_sites, month]
  # take only 1 month: min_unique_sites_month
  min_unique_sites_month <- head(unique(min_unique_sites_month), 1)
  # pass month to DT, grab these sites: tracked sites
  tracked_sites <- DT[month == min_unique_sites_month, site]
  # track only these XYZ sites over time: DT_filtered
  DT_filtered <- DT[site %in% tracked_sites]
  # return DT
  return(DT_filtered)
}

# loop above function n times: filter_sites_n_times
filter_sites_n_times <- function(DT, n) {
  if (n == 0) {
    return(DT)
  }
  Recall(filter_sites(DT), n - 1)
}

# apply: sites_DT
sites_DT[, .(unique_sites = uniqueN(site)), by = .(month)] # before
sites_DT <- filter_sites_n_times(sites_DT, 10) # loop it approx. 10 times
sites_DT[, .(unique_sites = uniqueN(site)), by = .(month)] # after: correct

# check 10 random sites: correct
sites_DT[site == "amazon.de", .(site, month)]
sites_DT[site == "wikipedia.org", .(site, month)]
sites_DT[site == "google.com", .(site, month)]
sites_DT[site == "xing.com", .(site, month)]
sites_DT[site == "geocaching.com", .(site, month)]
sites_DT[site == "linkedin.com", .(site, month)]
sites_DT[site == "hse24.de", .(site, month)]
sites_DT[site == "office.com", .(site, month)]
sites_DT[site == "theguardian.com", .(site, month)]
sites_DT[site == "spotify.com", .(site, month)]


# unique N sites visited over the years?
uniqueN(sites_DT, by = "site")

# unique categories visited over the years?
uniqueN(sites_DT, by = "category")

# add IP addresses ----
# of servers according to host domain using NS lookup
# take only single IPv4 params from curl::nslookup(..., ipv4_only = T, multiple = F, error = F)
get_ipv4 <- function(domain) {
  res <- nslookup(domain, ipv4_only = T, multiple = F, error = F)
  ifelse(length(res) == 0, # needed to handle exceptions, like 'youtube-mp3.org' that have NULL length
    return("NA"),
    return(res)
  )
}

# test
get_ipv4("amazon.de")
get_ipv4("google.com")

# add ipv4s to all unique domains: unique_sites
# you do this to avoid 100k loop (time-consuming)
uniqueN(sites_DT$site)
unique_sites <- data.table("site" = unique(sites_DT$site))

# loop it, add ipv4s to all unique sites/domains
for (i in 1:nrow(unique_sites)) {
  ipv4 <- get_ipv4(unique_sites$site[i])
  unique_sites$ipv4[i] <- ipv4
  ui_todo("adding IPv4 to site: ")
  print(i)
  cat("from total number of sites:", nrow(unique_sites), "\n")
}
ui_done("all IPv4s added!")

# join unique ipv4s to all sites
sites_DT <- merge(sites_DT, unique_sites, by = "site")
sites_DT

# reorder by year
setorder(sites_DT, "month")
sites_DT[, "month"]

# add server locations ----
# using up-to-date IP to city DB-IP (free, most comprehensive DB): .mmdb file
# TODO: download lastest DB from: https://tinyurl.com/vwznymv
# put it to "data" folder in RStudio Project directory
# connect to it
folder <- c("./data/db_ip/")
geo_db <- grep(".mmdb$", list.files(folder), value = T)
geo_db <- paste0(folder, geo_db) # full path
geo_db # full name
# choose cols:
# focus only on country-level info: https://bit.ly/3cxBb86
fetch_cols <- c("continent_name", "country_name", "country_code")
# add unique IPV4 countries: server_info
server_info <- setDT(sites_DT[, maxmind(ipv4, geo_db, fetch_cols)])
server_info
# merge with original dataset: sites_DT
dim(sites_DT)
sites_DT <- cbind(sites_DT, server_info)
dim(sites_DT)
sites_DT[, .(site, ipv4, continent_name, country_name, country_code)]
# rename cols: 'server_'
old_cols <- c("continent_name", "country_name", "country_code")
new_cols <- paste("server_", old_cols, sep = "")
setnames(sites_DT, old_cols, new_cols)
colnames(sites_DT)

# N captured server locations?
captured_servers <- unique(sites_DT, by = c("server_country_name", "server_country_code"))
captured_servers[, .(server_country_name, server_country_code)]
mean(is.na(sites_DT$server_country_name))
any(is.na(sites_DT$server_country_code))

# split control/treatment groups by server location ----
# mark websites based on server location in 'server_country_code': is_treated_server
# ISO 3166 alpha 2 codes: https://bit.ly/2RNkEVG
# territorial scope (Article 3): https://bit.ly/2VyZpIg
# list includes EU members, 9 outermost regions, as well as the EEA (Article 3.3), similar to: https://bit.ly/2yuhiAs (excluding Montenegro)
sites_DT[, is_treated_server := fifelse(
  server_country_code == "AT" | # Austria
    server_country_code == "BE" | # Belgium
    server_country_code == "BG" | # Bulgaria
    server_country_code == "HR" | # Croatia
    server_country_code == "CY" | # Cyprus
    server_country_code == "CZ" | # Czechia
    server_country_code == "DN" | # Denmark
    server_country_code == "EE" | # Estonia
    server_country_code == "FI" | # Finland
    server_country_code == "AX" | # Åland Islands (Finland)
    server_country_code == "FR" | # France (includes Clipperton Island)
    server_country_code == "GF" | # French Guiana (France)
    server_country_code == "GP" | # Guadeloupe (France)
    server_country_code == "MQ" | # Martinique (France)
    server_country_code == "YT" | # Mayotte (France)
    server_country_code == "RE" | # Réunion (France)
    server_country_code == "MF" | # Saint Martin (France / Netherlands)
    server_country_code == "DE" | # Germany
    server_country_code == "GB" | # United Kingdom of Great Britain and Northern Ireland
    server_country_code == "GI" | # Gibraltar (United Kingdom of Great Britain and Northern Ireland)
    server_country_code == "GR" | # Greece
    server_country_code == "HU" | # Hungary
    server_country_code == "IE" | # Ireland
    server_country_code == "IT" | # Italy
    server_country_code == "LV" | # Latvia
    server_country_code == "LT" | # Lithuania
    server_country_code == "LU" | # Luxembourg
    server_country_code == "MT" | # Malta
    server_country_code == "NL" | # Netherlands
    server_country_code == "PL" | # Poland
    server_country_code == "PT" | # Portugal
    server_country_code == "RO" | # Romania
    server_country_code == "SK" | # Slovakia
    server_country_code == "SI" | # Slovenia
    server_country_code == "ES" | # Spain
    server_country_code == "SE" | # Sweden
    server_country_code == "NO" | # Norway, as member of EER: https://bit.ly/2xyXziW
    server_country_code == "IS" | # Iceland, as member of EER: https://bit.ly/2xyXziW
    server_country_code == "LI", # Liechtenstein, as member of EER: https://bit.ly/2xyXziW
  1,
  0
)][]

# correct labeling?
unique(sites_DT$is_treated_server)

# number of treated / control sites?
sites_DT[, .(N = uniqueN(site)), by = .(is_treated_server)][order(is_treated_server)]


# mark server locations: server_in_EU_EEA (firm location proxy) ----
unique(sites_DT$server_continent_name)
sites_DT[, server_in_EU_EEA := as.factor(fifelse(
  server_country_code == "AT" | # Austria
    server_country_code == "BE" | # Belgium
    server_country_code == "BG" | # Bulgaria
    server_country_code == "HR" | # Croatia
    server_country_code == "CY" | # Cyprus
    server_country_code == "CZ" | # Czechia
    server_country_code == "DN" | # Denmark
    server_country_code == "EE" | # Estonia
    server_country_code == "FI" | # Finland
    server_country_code == "AX" | # Åland Islands (Finland)
    server_country_code == "FR" | # France (includes Clipperton Island)
    server_country_code == "GF" | # French Guiana (France)
    server_country_code == "GP" | # Guadeloupe (France)
    server_country_code == "MQ" | # Martinique (France)
    server_country_code == "YT" | # Mayotte (France)
    server_country_code == "RE" | # Réunion (France)
    server_country_code == "MF" | # Saint Martin (France / Netherlands)
    server_country_code == "DE" | # Germany
    server_country_code == "GB" | # United Kingdom of Great Britain and Northern Ireland
    server_country_code == "GI" | # Gibraltar (United Kingdom of Great Britain and Northern Ireland)
    server_country_code == "GR" | # Greece
    server_country_code == "HU" | # Hungary
    server_country_code == "IE" | # Ireland
    server_country_code == "IT" | # Italy
    server_country_code == "LV" | # Latvia
    server_country_code == "LT" | # Lithuania
    server_country_code == "LU" | # Luxembourg
    server_country_code == "MT" | # Malta
    server_country_code == "NL" | # Netherlands
    server_country_code == "PL" | # Poland
    server_country_code == "PT" | # Portugal
    server_country_code == "RO" | # Romania
    server_country_code == "SK" | # Slovakia
    server_country_code == "SI" | # Slovenia
    server_country_code == "ES" | # Spain
    server_country_code == "SE" | # Sweden
    server_country_code == "NO" | # Norway, as member of EEA (mid-July '18): https://bit.ly/2xyXziW
    server_country_code == "IS" | # Iceland, as member of EEA (mid-July '18): https://bit.ly/2xyXziW
    server_country_code == "LI", # Liechtenstein, as member of EEA (mid-July '18): https://bit.ly/2xyXziW
  1,
  0
))]
levels(sites_DT$server_in_EU_EEA) <- c("non-EU", "EU")
sites_DT[, .N, by = .(country, server_in_EU_EEA, is_treated_server)]

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

# any Norway, Iceland, Liechtenstein sites?
sites_DT[server_country_code == "NO"]
sites_DT[server_country_code == "IS"]
sites_DT[server_country_code == "LI"]

# add main time dummy/indicator: time_dummy ----
# GDPR implementation: 25.05.18
sites_DT[, time_dummy := as.integer(fifelse(
  month >= "2018-05-31", # May '18
  1,
  0
))][]


# add pre-treatment time indicator: time_pre ----
sites_DT[, time_pre := as.integer(fifelse(
  month <= "2018-04-30", # May '18 but leave April as baseline
  1,
  0
))][]


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


# split control/treatment groups by ccTLD ----
# add domain suffixes to merged_DT: domain_suffix_DT
# i.e. extract TLD's from 'site'
domain_suffix_DT <- setDT(sites_DT[, suffix_extract(site)])
domain_suffix_DT

# drop not needed cols: subdomain, domain
domain_suffix_DT[, c('subdomain', 'domain') := NULL][]

# add suffix_country: https://t.ly/MrHE
nrow(domain_suffix_DT)
suffix_countries <- fread('https://raw.githubusercontent.com/incognico/list-of-top-level-domains/master/assets/tld-desc.csv',
                          header = F, col.names = c('suffix', 'suffix_country'))
suffix_countries[domain_suffix_DT, on='suffix'] # test
domain_suffix_DT <- suffix_countries[domain_suffix_DT, on='suffix']

# cbind to original dataset: sites_DT
nrow(domain_suffix_DT)
nrow(sites_DT)
domain_suffix_DT[, .(host)]
sites_DT[, .(site)]
sites_DT <- cbind(sites_DT, domain_suffix_DT)
sites_DT

# drop not needed col: host (same as site)
sites_DT[, host := NULL][]

# check all suffixes
sort(unique(sites_DT$suffix))

# find issues
sites_DT[is.na(suffix_country==T), unique(suffix)]

# fix co.uk
sites_DT[suffix =='co.uk', .(site, suffix, suffix_country)][1:2]
sites_DT[suffix =='co.uk', suffix_country := 'UK']
# fix de.com
sites_DT[suffix =='de.com', .(site, suffix, suffix_country)][1:2]
sites_DT[suffix =='de.com', suffix_country := 'Germany']
# fix com.tr
sites_DT[suffix =='com.tr', .(site, suffix, suffix_country)][1:2]
sites_DT[suffix =='com.tr', suffix_country := 'Turkey']

# check suffix countries
sort(unique(sites_DT$suffix_country))

# check exceptions
# exceptions: 01net.com, buttinette.com, clubic.com, deichmann.com, 
#             eronity.com, handelsblatt.com, hartgeld.com, kaufmich.com,
#             lufthansa.com, pons.com, pr0gramm.com, ryanair.com, 
#             shop-apotheke.com, sportscheck.com, spox.com, wetter.com, xing.com
sites_DT[suffix_country=='Commercial organizations', unique(site)]
# exceptions: bauhaus.info
sites_DT[suffix_country=='Informational sites', unique(site)]
# exceptions: faz.net, finanzen.net, gutefrage.net
sites_DT[suffix_country=='Network', unique(site)]
# exceptions: leo.org, serienjunkies.org 
sites_DT[suffix_country=='Non-profit organizations', unique(site)]
# exceptions: arte.tv
sites_DT[suffix_country=='Tuvalu', unique(site)]
# exceptions: vipergirls.to
sites_DT[suffix_country=='Tonga (Kingdom of)', unique(site)] 


# rename countries: suffix_country_clean
sites_DT[suffix_country=='Austria (Republic of)', suffix_country_clean := 'Austria']
sites_DT[suffix_country=='Belgium (Kingdom of)', suffix_country_clean := 'Belgium']
sites_DT[suffix_country=='Cocos (Keeling) Islands (Territory of the)', suffix_country_clean := 'Cocos Islands'][]
sites_DT[suffix_country=='Commercial organizations', suffix_country_clean := fifelse(
  # EU commercial exceptions
  site == '01net.com' |
  site == 'buttinette.com' |
  site == 'clubic.com' |
  site == 'deichmann.com' |
  site == 'eronity.com' |
  site == 'handelsblatt.com' |
  site == 'hartgeld.com' |
  site == 'kaufmich.com' |
  site == 'pr0gramm.com' |
  site == 'shop-apotheke.com' |
  site == 'sportscheck.com' |
  site == 'spox.com' |
  site == 'wetter.com',
  'EU commercial',
  'US commercial')]
sites_DT[suffix_country=='European Union', suffix_country_clean := 'EU']
sites_DT[suffix_country=='France (French Republic)', suffix_country_clean := 'France']
sites_DT[suffix_country=='Germany (Federal Republic of)', suffix_country_clean := 'Germany']
sites_DT[suffix_country=='Germany', suffix_country_clean := 'Germany']
sites_DT[suffix_country=='Informational sites', suffix_country_clean := 'EU informational']
sites_DT[suffix_country=='Montenegro', suffix_country_clean := 'Montenegro']
sites_DT[suffix_country == 'Network', suffix_country_clean := fifelse(
  # EU network exceptions
  site == 'faz.net' |
  site == 'finanzen.net' |
  site == 'gutefrage.net',
  'EU network',
  'US network')]
sites_DT[suffix_country == 'Non-profit organizations', suffix_country_clean := fifelse(
  # EU non-profit exception
  site == 'leo.org' |
  site == 'serienjunkies.org',
  'EU non-profit',
  'US non-profit')]
sites_DT[suffix_country=='Russia (Russian Federation)', suffix_country_clean := 'Russia']
sites_DT[suffix_country=='Sint Maarten', suffix_country_clean := 'Netherlands']
sites_DT[suffix_country=='Switzerland (Swiss Confederation)', suffix_country_clean := 'Switzerland']
sites_DT[suffix_country=='Tonga (Kingdom of)', suffix_country_clean := fifelse(
  # EU Tonga exceptions
  site == 'vipergirls.to',
  'US Tonga',
  'EU Tonga')]
sites_DT[suffix_country=='Tuvalu', suffix_country_clean := fifelse(
  # EU Tuvalu exceptions
  site == 'arte.tv', 
'EU TV', 
'US TV')]
sites_DT[suffix_country=='US government', suffix_country_clean := 'US government']
sites_DT[suffix_country=='UK', suffix_country_clean := 'UK']
sites_DT[suffix_country=='Turkey', suffix_country_clean := 'Turkey']

# check
sites_DT[, sort(unique(suffix_country_clean))]


# group under single EU / US umbrellas: suffix_country_group
sites_DT[, sort(unique(suffix))]
sites_DT[suffix == 'at', suffix_country_group := 'Austria']
sites_DT[suffix == 'be', suffix_country_group := 'Belgium']
sites_DT[suffix == 'cc', suffix_country_group := 'Cocos Islands']
sites_DT[suffix == 'ch', suffix_country_group := 'Switzerland']
sites_DT[suffix == 'co.uk', suffix_country_group := 'UK']
sites_DT[suffix == 'com', suffix_country_group := 'Commercial']
sites_DT[suffix == 'com.tr', suffix_country_group := 'Turkey']
sites_DT[suffix == 'de', suffix_country_group := 'Germany']
sites_DT[suffix == 'de.com', suffix_country_group := 'Germany']
sites_DT[suffix == 'eu', suffix_country_group := 'EU']
sites_DT[suffix == 'fr', suffix_country_group := 'France']
sites_DT[suffix == 'gov', suffix_country_group := 'Government']
sites_DT[suffix == 'info', suffix_country_group := 'Informational']
sites_DT[suffix == 'me', suffix_country_group := 'Montenegro']
sites_DT[suffix == 'net', suffix_country_group := 'Network']
sites_DT[suffix == 'org', suffix_country_group := 'Organization']
sites_DT[suffix == 'ru', suffix_country_group := 'Russia']
sites_DT[suffix == 'sx', suffix_country_group := 'UK']
sites_DT[suffix == 'to', suffix_country_group := 'Tonga']
sites_DT[suffix == 'tv', suffix_country_group := 'Tuvalu']


# group under single EU / US umbrellas: suffix_country_group_robust
sites_DT[suffix_country_clean == 'Austria', suffix_country_group_robust := 'Austria']
sites_DT[suffix_country_clean == 'Belgium', suffix_country_group_robust := 'Belgium']
sites_DT[suffix_country_clean == 'Cocos Islands', suffix_country_group_robust := 'Cocos Islands']
sites_DT[suffix_country_clean == 'EU', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU commercial', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU informational', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU network', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU non-profit', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU Tonga', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'EU TV', suffix_country_group_robust := 'EU']
sites_DT[suffix_country_clean == 'France', suffix_country_group_robust := 'France']
sites_DT[suffix_country_clean == 'Germany', suffix_country_group_robust := 'Germany']
sites_DT[suffix_country_clean == 'Montenegro', suffix_country_group_robust := 'Montenegro']
sites_DT[suffix_country_clean == 'Netherlands', suffix_country_group_robust := 'Netherlands']
sites_DT[suffix_country_clean == 'Russia', suffix_country_group_robust := 'Russia']
sites_DT[suffix_country_clean == 'Switzerland', suffix_country_group_robust := 'Switzerland']
sites_DT[suffix_country_clean == 'Turkey', suffix_country_group_robust := 'Turkey']
sites_DT[suffix_country_clean == 'UK', suffix_country_group_robust := 'UK']
sites_DT[suffix_country_clean == 'US commercial', suffix_country_group_robust := 'US']
sites_DT[suffix_country_clean == 'US government', suffix_country_group_robust := 'US']
sites_DT[suffix_country_clean == 'US network', suffix_country_group_robust := 'US']
sites_DT[suffix_country_clean == 'US non-profit', suffix_country_group_robust := 'US']
sites_DT[suffix_country_clean == 'US Tonga', suffix_country_group_robust := 'US']
sites_DT[suffix_country_clean == 'US TV', suffix_country_group_robust := 'US']
sites_DT[, sort(unique(suffix_country_group_robust))]


# group to EU / non-EU: suffix_country_aggregate
sites_DT[, sort(unique(suffix))]
sites_DT[, suffix_country_aggregate := fifelse(
    suffix == "at" | # Austria
    suffix == "be" | # Belgium
    suffix == "co.uk" | # United Kingdom of Great Britain and Northern Ireland
    suffix == "de" | # Germany (1)
    suffix == "de.com" | # Germany (2)
    suffix == "eu" | # general EU domain
    suffix == "fr" | # France
    suffix == "sx", # Saint Martin (France / Netherlands)
  "EU",
  "non-EU"
)]

# check
sites_DT[, unique(suffix_country_aggregate)]


# redefine control / treated groups: is_treated_ccTLD
# reference: https://t.ly/N1lW
# simple heuristic:
# a site XYZ is "treated" if:
# its ccTLD involves EU/EEA domains (.nl, .de, etc.)
# otherwise "control"
# irrespective of server or users' location
sites_DT[, is_treated_ccTLD := fifelse(
    # its ccTLD includes EU/EEA domains (.nl, .de, etc.) 
    suffix == "at" | # Austria
    suffix == "be" | # Belgium
    suffix == "fr" | # France
    suffix == "sx" | # Saint Martin (France / Netherlands)
    suffix == "de" | # Germany (1)
    suffix == "de.com" | # Germany (2)
    suffix == "co.uk" | # United Kingdom of Great Britain and Northern Ireland
    suffix == "eu", # general EU domain
  1,
  0)][]


# check result of this control/treatment split
sites_DT[, .(N = uniqueN(site)), by=.(is_treated_ccTLD)]
sites_DT[, .(site, is_treated_ccTLD)]


# group to EU / non-EU: suffix_country_aggregate_robust
sites_DT[, sort(unique(suffix_country_group))]
sites_DT[, suffix_country_aggregate_robust := fifelse(
  suffix_country_group == 'Austria' |
  suffix_country_group == 'Belgium' |
  suffix_country_group == 'EU' |
  suffix_country_group == 'France' |
  suffix_country_group == 'Germany' |
  suffix_country_group == 'Netherlands' |
  suffix_country_group == 'UK',
  'EU',
  'non-EU'
)]

# check
sites_DT[, unique(suffix_country_aggregate_robust)]



# redefine control / treated groups: is_treated_ccTLD_robust
# robust heuristic
sites_DT[, sort(unique(suffix_country_clean))] # check groups
sites_DT[, is_treated_ccTLD_robust := fifelse(
    suffix_country_clean == 'Austria' |
    suffix_country_clean == 'Belgium' |
    suffix_country_clean == 'EU' |
    suffix_country_clean == 'EU commercial' |
    suffix_country_clean == 'EU informational' |
    suffix_country_clean == 'EU network' |
    suffix_country_clean == 'EU non-profit' |
    suffix_country_clean == 'EU Tonga' |
    suffix_country_clean == 'EU TV' |
    suffix_country_clean == 'France' |
    suffix_country_clean == 'Germany' |
    suffix_country_clean == 'Netherlands' |
    suffix_country_clean == 'UK',
  1,
  0)][]


# check result of this control/treatment split
sites_DT[, .(N = uniqueN(site)), by=.(is_treated_ccTLD_robust)]
sites_DT[, .(site, is_treated_ccTLD_robust)]





# recode chars/numerics to factors ----
# 1. country
# 2. site
# 3. category
# 4. ipv4
# 5. server_continent_name
# 6. server_country_name
# 7. server_country_code
# 8. is_treated_server
vars <- c(
  "country", "site", "category", "ipv4", "server_continent_name", "server_country_name",
  "server_country_code", "suffix", "suffix_country", "suffix_country_clean", 
  "suffix_country_group", "suffix_country_group_robust",
  "suffix_country_aggregate", "suffix_country_aggregate_robust"
)
sites_DT[, lapply(.SD, class), .SDcols = vars]
sites_DT[, (vars) := lapply(.SD, as.factor), .SDcols = vars]
sites_DT[, lapply(.SD, class), .SDcols = vars]
glimpse(sites_DT)

# number of sites visited by global users accross time?
sites_DT[, uniqueN(site)]

# number of sites in each category?
sites_DT[, .(n = uniqueN(site)), by = .(category)][order(-n)]

# number of treated / control sites?
sites_DT[, .(n = uniqueN(site)), by = .(is_treated_server)][order(is_treated_server)]

# treated / control sites per category?
sites_DT[, .(n = uniqueN(site)), by = .(category, is_treated_server)][order(category, is_treated_server)]

# save unique 447 websites (panel data)
panel_sites <- sites_DT[, .(site = unique(site))]


# export ----
# as .rds to 'global' folder in 'data/sample'
dir.create(file.path("./data//sample/global"), showWarnings = F)
saveRDS(sites_DT, file = "./data/sample/global/20200528_global_sites_merged.rds", compress = F)
saveRDS(panel_sites, file = "./data/sample/global/20200528_panel_sites.rds", compress = F)

# check
list.files("./data/sample/global")

# test loading: correct
rm(list = ls())
sites_DT <- readRDS("./data/sample/global/20200528_global_sites_merged.rds")
sites_DT
glimpse(sites_DT)
summary(sites_DT)





