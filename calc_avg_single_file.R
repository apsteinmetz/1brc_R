# 1 brc challenge
# load simulated weather station data and compute hi/lo/average temperature

library(tidyverse)
library(data.table)
library(dtplyr)
library(duckdb)
library(duckplyr)
library(polars)
library(tidypolars)
library(arrow)
library(tictoc)
library(microbenchmark)

num_records <- 1e7
num_files = 1


read_single_file <- function(n){
   cat(n)
   fread(paste0("data/chunk_",sprintf(paste0("%0",4, "d"), n),".txt"),
         header = FALSE,
         col.names = c("city", "temp"),
         colClasses = c("character","numeric"),
         sep=";",
         showProgress = TRUE)
}


make_full_dataset <- function(num_files){
   all_lines <- lapply(1:num_files, read_single_file) |>
      rbindlist()
   all_lines
}

expand_dataset <- function(all_lines,num_recs = num_records){
   all_lines[rep(1:nrow(all_lines),
                              num_recs/nrow(all_lines))]

}

expand_dataset_2 <- function(all_lines,num_recs = num_records){
   slice_sample(all_lines,n=num_recs,replace = TRUE)
}

cat("making dataset")
tictoc::tic()
tidy_df <- make_full_dataset(num_files) %>%
   expand_dataset_2() %>% as_tibble()
tictoc::toc()

# the standard by which all others are measured
# data.table -------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using data.table


do_data.table <- function(df = tidy_df) {
   if ("data.table" %in% class(df)) {
      df[, .(
         high = max(temp),
         low = min(temp),
         avg = mean(temp),
         n = .N
      ), by = city]
   } else{
      tidy_df |>
         as.data.table() |>
         lazy_dt(immutable = FALSE) |>
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

# BASE R -----------------------------------------------------------------------
do_base <- function() {
   agg_lines <- aggregate(temp ~ city, data = tidy_df,
                          FUN = function(x) c(summary=summary(x), n=length(x)))
   agg_lines <- cbind(agg_lines[-2],as.data.frame(agg_lines$temp))[c(-3,-4,-6)]
   names(agg_lines) <- c("city","min","avg","max")
   agg_lines[-5]
}



   # Rename columns
   colnames(summary_stats) <- c("city", "low", "avg", "high")

   # Return the result
   summary_stats
}

do_optimized()

# DPLYR -----------------------------------------------------------------------
do_dplyr <- function() {
   tidy_df |>
      as.data.frame() |>
      group_by(city) |>
      summarize(high = max(temp), low = min(temp), avg = mean(temp))
   }


# DUCkDB -----------------------------------------------------------------------
# compute the high/low /average temperature for each city from all_lines using duckdb

do_duckdb <- function(use_tidy = TRUE) {
   # duckdb_register(con, "duck_df",overwrite = TRUE, orig_df)
   if (use_tidy) {
      # convert tibble to duckdb table
      result  <- as_duckplyr_df(tidy_df) %>%
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp)
         )
   } else {
      con <- dbConnect(duckdb::duckdb())
      duckdb_register(con, "duck_df",overwrite = TRUE, tidy_df)
      # achieve the same result with dbGetQuery
      result <- dbGetQuery(
         con,
         "SELECT city, AVG(temp) as avg, MIN(temp) as low, MAX(temp) as high FROM duck_df GROUP BY city"
      )
      result
      dbDisconnect(con,shutdown = TRUE)
      rm(con)
   }
   return(result)
}

# POLARS  -------------------------------------------------------------------
do_polars <- function(df) {
   if("tbl" %in% class(df)){
      result <- df |>
         group_by(city) |>
         summarize(high = max(temp),
                   low = min(temp),
                   avg = mean(temp)) |>
         arrange(city)
      return(result)
   }
   result <- df$group_by("city")$agg(
      pl$col("temp")$sum()$alias("avg"),
      pl$col("temp")$min()$alias("low"),
      pl$col("temp")$max()$alias("high")
   )
   if(class(df) == "RPolarsLazyFrame"){
      result |> collect()
   } else {
      result
   }

   # as_tibble(result)
}

# TIDYOLARS  -------------------------------------------------------------------
# do_tidy_polars <- function(df_polars) {
#    tidy_df |>
#       group_by(city) |>
#       summarize(high = max(temp),
#                 low = min(temp),
#                 avg = mean(temp)) |>
#       arrange(city) |>
#       as_tibble()
# }

# autoplot(bm)
# clean up
# rm(polars_lf,polars_df)
gc()
# bm |> ggplot(aes(y = expr, x = time/1e8)) + geom_col() + theme_minimal() |>
#    labs(title = "Polars vs Tidypolars",
#         subtitle = "Time in seconds",
#         x = "Time (Seconds to Process 100 million records)"
#         y = "Function")


# ARROW -----------------------------------------------------------------------
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

cat("coecring tidy to data.table\n")
DT_df <- as.data.table(tidy_df)
cat("coecring tidy to polars\n")
polars_lazy <- tidy_df |> as_polars_lf()
polars_data <- tidy_df |> as_polars_df()
cat("coecring tidy to arrow\n")
arrow_df <- tidy_df |> arrow_table()

cat("running benchmark\n")
times <- 10
tm <-  microbenchmark(do_dplyr(),
                      do_data.table(DT_df),
                      do_data.table(tidy_df),
                      do_duckdb(use_tidy = FALSE),
                      do_duckdb(use_tidy = TRUE),
                      do_arrow(arrow_df),
                      do_arrow(tidy_df),
                      do_polars(polars_data),
                      do_polars(polars_lazy),
                      do_polars(tidy_df),
                      times = times)
if (times ==1){
   tm <- tm %>% as_tibble() %>% arrange(expr) %>%
      mutate(
         db = as.factor(
            c(
               "dplyr",
               "data.table","data.table",
               "duckdb","duckdb",
               "arrow","arrow",
               "polars","polars","polars")))
   tm |>
      ggplot(aes(fct_reorder(expr,time),time/1e9,fill=db)) + geom_col() +
      coord_flip() +
      labs(x = "Database Method",y="Seconds")

   tm |>
      ggplot(aes(expr,time/1e9,fill=db)) + geom_col() +
      coord_flip() +
      labs(x = "Database Method",y="Seconds")

} else autoplot(tm)

# rm(arrow_df,polars_data,polars_lazy)
gc()

