# FAST generation of random weather station data for 1BRC project
# using the furrr package for parallel processing
# and the future package for future plan
library(tidyverse)
library(rvest)
library(furrr)
library(future)

# Define constants
# one billion records
# ONE_BRP <- 1e9
ONE_BRP <- 1e3
CHUNK_SIZE <- 100
if( CHUNK_SIZE >= ONE_BRP){
   stop("CHUNK_SIZE must be less than or equal to ONE_BRP")
}

if (ONE_BRP %% CHUNK_SIZE != 0) {
   stop("CHUNK_SIZE must be divisible by ONE_BRP")
}

NUM_CHUNKS  <- ONE_BRP/CHUNK_SIZE

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
generate_single_observation <- function(){
   rownum <- sample(nrow(city_table), 1)
   city <- city_table$City[rownum]
   obs_temp <- rnorm(1, mean = city_table$Year[rownum], sd = 10)
   return(paste(city, round(obs_temp, 1), sep = ";"))
}


generate_multiple_observations <- function(n,chunk_size = CHUNK_SIZE){
   write_lines(paste(replicate(chunk_size, generate_single_observation()), collapse = "\n"),
               file = paste0("data/chunk_",sprintf(paste0("%0",8, "d"), n),".txt"),
               append = TRUE)
}


# Set up a future plan
plan(multisession)
# Generate weather station data
cat(" Starting jobs\n")
cat(paste(" Process will produce",NUM_CHUNKS,"files of",CHUNK_SIZE,"records each\n"))
tictoc::tic()
chunks_created <- future_walk(1:NUM_CHUNKS, generate_multiple_observations,.options = furrr_options(seed = 123))
tictoc::toc()

