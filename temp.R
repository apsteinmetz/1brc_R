con = dbConnect(duckdb(), ":memory:")
duckdb_register(con, "duck_df",overwrite = TRUE, orig_df)

do_duckdb <- function(orig_df = tidy_df,use_duckplyr = TRUE) {
   if (use_duckplyr) {
      result  <- tbl(con, "duck_df") %>%
         group_by(city) |>
         summarize(
            high = max(temp),
            low = min(temp),
            avg = mean(temp)
         )
   } else {
      # achieve the same result with dbGetQuery
      result <- dbGetQuery(
         con,
         "SELECT city, AVG(temp) as avg, MIN(temp) as low, MAX(temp) as high FROM duck_df GROUP BY city"
      )
      result <- result %>%
         collect() %>%
         as_tibble()
   }
   return(result)
}

do_duckdb()
dbDisconnect(con)
rm(con)
