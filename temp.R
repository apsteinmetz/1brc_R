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
      result <- result %>%
         as_tibble()
      dbDisconnect(con,shutdown = TRUE)
      rm(con)
   }
   return(result)
}

do_duckdb(use_tidy = TRUE)
do_duckdb(use_tidy = FALSE)
