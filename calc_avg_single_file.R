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


num_records <- 1e8
num_files = 100


read_single_file <- function(n){
   fread(paste0("data/chunk_",sprintf(paste0("%0",8, "d"), n),".txt"),
         header = FALSE,
         col.names = c("city", "temp"),
         colClasses = c("character","numeric"),
         sep=";")
}


make_full_dataset <- function(num_files){
   all_lines <- lapply(1:num_files, read_single_file) |>
      rbindlist()
   all_lines
}

expand_dataset <- function(all_lines){
   all_lines[rep(1:nrow(all_lines),
                              num_records/nrow(all_lines))]

}

tictoc::tic()
tidy_df <- make_full_dataset(num_files) |>
   expand_dataset()
tictoc::toc()
tidy_df <- as_tibble(tidy_df)

# the standard by which all others are measured
# data.table -------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using data.table

do_dt <- function() {
   tidy_df |> as.data.table() |>
   _[,.(high = max(temp), low = min(temp), avg = mean(temp), n = .N), by = city]
}

# BASE R -----------------------------------------------------------------------
do_base <- function() {
   agg_lines <- aggregate(temp ~ city, data = tidy_df,
                          FUN = function(x) c(summary=summary(x), n=length(x)))
   agg_lines <- cbind(agg_lines[-2],as.data.frame(agg_lines$temp))[c(-3,-4,-6)]
   names(agg_lines) <- c("city","min","avg","max","n")
   agg_lines[-5]
}

# DPLYR -----------------------------------------------------------------------
do_dplyr <- function() {
   tidy_df |>
      as.data.frame() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp))
   }

# DTPLYR -----------------------------------------------------------------------
do_dtplyr <- function() {
   tidy_df |>
      as.data.table() |>
      lazy_dt(immutable = FALSE) |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp), n = n()) |>
      collect()
}

# DUCkDB -----------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using duckdb
con = dbConnect(duckdb(), ":memory:")
duckdb_register(con, "duckdb_df", tidy_df)
duckdb_df <- tbl(con, "duckdb_df")

do_duckdb <- function(use_duckplyr = FALSE) {
   if (use_duckplyr) {
      duckdb_df |>
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp)
         )
   } else {
      # achieve the same result with dbGetQuery
      dbGetQuery(
         con,
         "SELECT city, AVG(temp) as avg, MIN(temp) as low, MAX(temp) as high FROM duckdb_df GROUP BY city"
      ) |>
         as_tibble()

   }
}


# POLARS  -------------------------------------------------------------------
polars_lf <- tidy_df |> as_polars_lf()
polars_df <- tidy_df |> as_polars_df()
do_polars <- function(df_polars) {
   # takes a polars dataframe and computes the high/low/average temperature for each city
   result <- df_polars$group_by("city")$agg(
      pl$col("temp")$sum()$alias("avg"),
      pl$col("temp")$min()$alias("low"),
      pl$col("temp")$max()$alias("high")
   )
   if(class(df_polars) == "RPolarsLazyFrame"){
      result |> collect()
   } else {
      result
   }

   # as_tibble(result)
}

# TIDYOLARS  -------------------------------------------------------------------
do_tidy_polars <- function(df_polars) {
   tidy_df |>
      group_by(city) |>
      summarize(high = max(temp),
                low = min(temp),
                avg = mean(temp)) |>
      arrange(city) |>
      as_tibble()
}

# autoplot(bm)
# clean up
# rm(polars_lf,polars_df)
gc()
# bm |> ggplot(aes(y = expr, x = time/1e8)) + geom_col() + theme_minimal() |>
#    labs(title = "Polars vs Tidypolars",
#         subtitle = "Time in seconds",
#         x = "Time (Seconds to Process 100 million records)",
#         y = "Function")


# ARROW -----------------------------------------------------------------------
arrow_df <- tidy_df |> arrow_table()
do_arrow <- function(df_arrow) {
   if ("ArrowObject" %in% class(df_arrow)) {
      df_arrow |>
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp),
            n = n()
         ) |>
         collect()
   } else {
      df_arrow |>
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


tm <-  microbenchmark(do_dplyr(),
                      do_dtplyr(),
                      do_dt(),
                      do_duckdb(use_duckplyr = FALSE),
                      do_duckdb(use_duckplyr = TRUE),
                      do_arrow(tidy_df),
                      do_arrow(arrow_df),
                      do_polars(polars_df),
                      do_tidy_polars(polars_df),
                      times = 1)
tm
# autoplot(tm)
as_tibble(tm) |> ggplot(aes(expr,time)) + geom_col()
rm(arrow_df,polars_df,polars_lf)
dbDisconnect(con, shutdown=TRUE)
rm(con)
rm(duckdb_df)
gc()




