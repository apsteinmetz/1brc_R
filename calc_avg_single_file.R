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
library(tictoc)
library(microbenchmark)


num_files <- 100 # 100 files of 10mm records each

read_single_file <- function(n){
   fread(paste0("data/chunk_",sprintf(paste0("%0",8, "d"), n),".txt"),
         header = FALSE,
         col.names = c("city", "temp"),
         colClasses = c("character","numeric"),
         sep=";")
}


make_full_dataset <- function(num_files,expansion_factor=1){
   all_lines <- lapply(1:num_files, read_single_file) |>
      rbindlist()
   all_lines <- all_lines[rep(1:nrow(all_lines), expansion_factor),]


   all_lines
}

tictoc::tic()
measurements <- make_full_dataset(num_files,expansion_factor = 1)
tictoc::toc()
measurements <- as_tibble(measurements)

# the standard by which all others are measured
# data.table -------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using data.table

do_dt <- function() {
   measurements |> as.data.table() |>
   _[,.(high = max(temp), low = min(temp), avg = mean(temp), n = .N), by = city]
}
tic()
do_dt()
toc()


# BASE R -----------------------------------------------------------------------
do_base <- function() {
   agg_lines <- aggregate(temp ~ city, data = measurements,
                          FUN = function(x) c(summary=summary(x), n=length(x)))
   agg_lines <- cbind(agg_lines[-2],as.data.frame(agg_lines$temp))[c(-3,-4,-6)]
   names(agg_lines) <- c("city","min","avg","max","n")
   agg_lines[-5]
}

tic()
do_base() |> head()
toc()

# DPLYR -----------------------------------------------------------------------
do_dplyr <- function() {
   measurements |>
      as.data.frame() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp))
   }

do_dplyr()
# DTPLYR -----------------------------------------------------------------------
do_dtplyr <- function() {
   measurements |>
      as.data.table() |>
      lazy_dt(immutable = FALSE) |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n()) |>
      collect()
}

tic()
do_dtplyr()
toc()
# DUCkDB -----------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using duckdb
do_duck_db <- function() {
   measurements |>
      as.data.frame() |>
      as_duckplyr_df() |>
      group_by(city) |>
      summarize(
         high = max(temp),
         low = min(temp),
         avg = mean(temp),
         n = n()
      )
}
do_duck_db()

# TIDYOLARS  -------------------------------------------------------------------
do_polars <- function() {
   measurements |>
      as_polars_df() |>
      group_by(city) |>
      summarize(high = max(temp),
                low = min(temp),
                avg = mean(temp)) |>
      arrange(city) |>
      as_tibble()
}

do_polars()
# ARROW -----------------------------------------------------------------------
do_arrow <- function(m = measurements) {
   if ("ArrowObject" %in% class(m)) {
      print("Native Arrow")
      m |>
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp),
            n = n()
         ) |>
         collect()
   } else {
      m |>
         arrow_table() |>
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp),
            n = n()
         ) |>
         collect()
   }
}

measurements <- measurements |> arrow_table()
tic()
do_arrow(measurements)
toc()
measurements <- measurements |> as_tibble()

tm <-  microbenchmark(do_dplyr(), do_dtplyr(), do_dt(), do_duck_db(), do_arrow(), do_polars(),times = 10)
tm
autoplot(tm)
as_tibble(tm) |> ggplot(aes(expr,time)) + geom_col()

