# FAST generation of random weather station data for 1BRC project
# using the furrr package for parallel processing
# and the future package for future plan
library(tidyverse)
library(rvest)
library(furrr)
library(data.table)
library(progressr)

# Define constants
# one billion records
# ONE_BRP <- 1e9
ONE_BRP <- 1e9
# create single file when chunk size is equal to ONE_BRP
CHUNK_SIZE <- 1e7
if( CHUNK_SIZE > ONE_BRP){
   stop("CHUNK_SIZE must be less than or equal to ONE_BRP")
}

if (ONE_BRP %% CHUNK_SIZE != 0) {
   stop("CHUNK_SIZE must be divisible by ONE_BRP")
}

NUM_CHUNKS  <- ONE_BRP/CHUNK_SIZE
WRITE_EACH_CHUNK = FALSE

if (file.exists("weather_stations.csv")) {
   city_table <- read_csv("weather_stations.csv",
                          show_col_types = FALSE)

} else {
   # retrieve table from https://en.wikipedia.org/wiki/List_of_cities_by_average_temperature using rvest package
   # install.packages("rvest")
   continents <- ""
   url <-
      "https://en.wikipedia.org/wiki/List_of_cities_by_average_temperature"
   webpage <- read_html(url)
   city_table <- html_nodes(webpage, "table") %>%
      html_table(fill = TRUE) %>%
      bind_rows() |>
      as_tibble() |>
      select(-Ref.) |>
      # remove all numbers in parentheses from all columns using across() and str_remove()
      mutate(across(everything(), \(x) str_remove_all(x, "\\(.*\\)"))) |>
      # mutate to numeric all columns except City and Country
      mutate(across(-c(City, Country), as.numeric)) |>
      # remove all rows with NA values
      drop_na()
   # write the table to a csv file
   write_csv(city_table, "weather_stations.csv")

}

# Define function to generate weather station data
generate_observations <- function(n){
   rownum <- sample(nrow(city_table), n, replace = TRUE)
   city <- city_table$City[rownum]
   obs_temp <- rnorm(n, mean = city_table$Year[rownum], sd = 5)
   return(as.data.frame(paste(city, round(obs_temp, 1), sep = ";")))
}


# -------------------------------------------------------------
# sequential write of single file
generate_measurement_file <- function(file_name = "data/measurements.txt",
                                      records = ONE_BRP,
                                      chunk_size = CHUNK_SIZE){
   if (file.exists("data/measurements.txt")) {
       file.remove("data/measurements.txt")
   }
   num_chunks <- records/chunk_size
   cat(paste("Creating measurement file ",file_name," with",format(records,big.mark = ","),"measurements...\n"))
   tictoc::tic()
   for (i in 1:num_chunks){
      measurement <- generate_observations(chunk_size)
      cat(paste("Writing chunk ",i," of ",num_chunks,"\n"))
      fwrite(measurement, file = file_name,append = TRUE,showProgress = TRUE)
   }
   tictoc::toc()
}

generate_measurement_file()

