# 1 brc challenge
# load simulated weather station data and compute hi/lo/average temperature

library(tidyverse)
library(furrr)
library(future)
library(progressr)
library(data.table)
library(dtplyr)
library(duckdb)
library(duckplyr)
library(polars)
library(tidypolars)
library(arrow)
library(microbenchmark)

num_files <- 100 # 100 files of 10mm records each

# BASE R -----------------------------------------------------------------------
do_base <- function() {
get_agg_weather_base <- function(n) {
   p()
   all_lines_base <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"),
                           header = FALSE, sep=";",
                           col.names = c("city", "temp"),
                           colClasses = c("character","numeric")) |>
      as.data.frame()
   #get summary statistics grouped by city using base R
   agg_lines <- aggregate(temp ~ city, data = all_lines_base,
                          FUN = function(x) c(summary=summary(x), n=length(x)))
   cbind(agg_lines[-2],as.data.frame(agg_lines$temp))[c(-3,-4,-6)]
}


plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_base) |>
      list_rbind()

})

names(agg_weather) <- c("city","min","avg","max","n")
agg_base <- agg_weather
agg_base$avg=with(agg_base,tapply(avg*n,city,sum)/tapply(n,city,sum))[agg_weather$city]
agg_base$min=with(agg_base,tapply(min,city,min))[agg_base$city]
agg_base$max=with(agg_base,tapply(max,city,max))[agg_base$city]
agg_base <- unique(agg_base[,1:4])

tictoc::toc()

plan(sequential)
}
do_base()


# DPLYR -----------------------------------------------------------------------
do_dplyr <- function() {
get_agg_weather_dplyr <- function(n) {
   p()
   all_lines_dplr <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp")) |>
      as.data.frame() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n())
   all_lines_dplr
}

plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_dplyr) |>
      list_rbind()
})
agg_weather |> group_by(city) |>
   summarize(high = max(high), low = min(low), avg = weighted.mean(avg,n))
tictoc::toc()

plan(sequential)
}
do_dplyr()
# DTPLYR -----------------------------------------------------------------------
do_dtplyr <- function() {
get_agg_weather_dtplyr <- function(n) {
   p()
   all_lines_dtplyr <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp")) |>
      # as.data.frame() |>
      lazy_dt(immutable = FALSE) |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n())
   collect(all_lines_dtplyr)
}


plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_dtplyr) |>
      list_rbind()
})
agg_weather |> group_by(city) |>
   summarize(high = max(high), low = min(low), avg = weighted.mean(avg,n))
tictoc::toc()

plan(sequential)
}
do_dtplyr()
# data.table -------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using data.table
do_dt <- function() {
get_agg_weather_dt <- function(n) {
   p()
   all_lines.dt <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp"))
   all_lines.dt[, .(high = max(temp), low = min(temp), avg = mean(temp), n = .N), by = city]
}

plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_dt) |>
      list_rbind()
})
agg_weather[, .(high = max(high), low = min(low), avg = weighted.mean(avg, n)), by = city] |>
   as_tibble()
tictoc::toc()

plan(sequential)
}
do_dt()

# DUCkDB -----------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using duckdb
do_duck_db <- function() {

   get_agg_weather_duck <- function(n) {
   p()
   all_lines_duck <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp")) |>
      as.data.frame() |>
      as_duckplyr_df() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n())
   all_lines_duck
}

plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_duck) |>
      list_rbind()
})
agg_weather |> group_by(city) |>
   summarize(high = max(high), low = min(low), avg = weighted.mean(avg,n)) |>
   as_tibble()
tictoc::toc()
}
do_duck_db()

# TIDYOLARS  -----------------------------------------------------------------------
do_polars <- function(){
get_agg_weather_polars <- function(n) {
   p()
   all_lines_polar <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp")) |>
      # as.data.frame() |>
      as_polars_df() |>
      group_by(city)
   # no count in summarize yest
   agg1 <- all_lines_polar |> count() |> arrange(city)
   agg2 <- all_lines_polar |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp)) |>
      arrange(city) |>
      bind_cols_polars(select(agg1,-city))
   agg2
}

# tidypolars breaks on multisessin disqualified
plan(sequential)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_polars,.options = furrr_options(packages = "tidypolars")) |>
   bind_rows_polars()
})

agg_weather |>
   as_tibble() |>
   group_by(city) |>
   summarize(high = max(high), low = min(low), avg = weighted.mean(avg,n))
tictoc::toc()

plan(sequential)
}
do_polars()
# ARROW -----------------------------------------------------------------------
do_arrow <- function(){
get_agg_weather_arrow <- function(n) {
   p()
   all_lines_arrow <- fread(paste0("data/chunk_", sprintf("%08d", n), ".txt"), header = FALSE, sep=";",col.names = c("city", "temp")) |>
      arrow_table() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n())
   collect(all_lines_arrow)
}

plan(multisession)
tictoc::tic()
with_progress({
   p <- progressr::progressor(num_files)
   agg_weather <- future_map(1:num_files, get_agg_weather_arrow) |>
      list_rbind()
})
agg_weather |> group_by(city) |>
   summarize(high = max(high), low = min(low), avg = weighted.mean(avg,n))
tictoc::toc()

plan(sequential)
}
do_arrow()

tm <-  microbenchmark(do_base(),do_dplyr(), do_dtplyr(), do_dt(), do_duck_db(), do_arrow(), do_polars(),times = 1)
tm
as_tibble(tm) |> ggplot(aes(expr,time)) + geom_col()

