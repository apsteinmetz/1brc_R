do_duckdb <- function(df = tidy_df, use_tidy = TRUE) {
   # duckdb_register(con, "duck_df",overwrite = TRUE, orig_df)
   if (use_tidy) {
      if ("duckplyr_df" %in% class(df)) {
         result <- df |>
            group_by(city) |>
            summarize(
               high = max(temp),
               low = min(temp),
               avg = mean(temp)
            )
         return(result)
      } else {
         result <- as_duckplyr_df(df) |>
            group_by(city) |>
            summarize(
               high = max(temp),
               low = min(temp),
               avg = mean(temp)
            )
         return(result)
      }
   } else {
      con <- dbConnect(duckdb::duckdb())
      duckdb_register(con, "duck_df", overwrite = TRUE, df)
      # achieve the same result with dbGetQuery
      result <- dbGetQuery(
         con,
         "SELECT city, AVG(temp) as avg, MIN(temp) as low, MAX(temp) as high FROM duck_df GROUP BY city"
      )
      result
      dbDisconnect(con, shutdown = TRUE)
      rm(con)
   }
   return(result)
}

duck_df <- as_duckplyr_df(tidy_df)

bm_duckdb <- microbenchmark(do_duckdb(tidy_df,use_tidy = FALSE),
                            do_duckdb(tidy_df,use_tidy = TRUE),
                            do_duckdb(duck_df,use_tidy = TRUE),
                            times = 1, unit="seconds")
bm_duckdb


microbenchmark(df <- tidy_df,df <- as_duckplyr_df(tidy_df),times = 100, unit="microseconds")
