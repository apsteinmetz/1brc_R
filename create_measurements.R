# FAST generation of random weather station data for 1BRC project
# using the furrr package for parallel processing
# and the future package for future plan
library(tidyverse)
library(rvest)
library(furrr)
library(future)
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
generate_single_observation <- function(n = 1){
   rownum <- sample(nrow(city_table), 1)
   city <- city_table$City[rownum]
   obs_temp <- rnorm(1, mean = city_table$Year[rownum], sd = 10)
   return(paste(city, round(obs_temp, 1), sep = ";"))
}


generate_multiple_observations <- function(n,chunk_size = CHUNK_SIZE,write_each_chunk = TRUE){
   p()
   if (write_each_chunk) {
      write_lines(paste(replicate(chunk_size, generate_single_observation()), collapse = "\n"),
                  file = paste0("data/chunk_",sprintf(paste0("%0",8, "d"), n),".txt"),
                  append = FALSE)
   } else {
      return(paste(replicate(chunk_size, generate_single_observation()), collapse = "\n"))
   }
}


# Set up a future plan
# plan(sequential)
plan(multisession)

# Generate weather station data
# write each chunk to a separate file
# cat(" Starting jobs\n")
# cat(paste(" Process will produce",NUM_CHUNKS,"chunks of",CHUNK_SIZE,"records each\n"))
# tictoc::tic()
# chunks_combined <- future_map(1:NUM_CHUNKS, generate_multiple_observations,.options = furrr_options(seed = 123))
# write_lines(chunks_combined,
#             file = paste0("data/chunk_all.txt"),
#             append = FALSE)
#
# tictoc::toc()

# Generate weather station data with future deciding chunk size
# cat(" Starting jobs multisession\n")
# tictoc::tic()
# chunks_combined <- future_map(1:ONE_BRP, generate_single_observation,
#                                  .progress = TRUE,
#                                  .options = furrr_options(seed = 123))
#
# print("Writing to file")
#    write_lines(chunks_combined,
#             file = paste0("data/chunk_all.txt"),
#             append = FALSE)
# tictoc::toc()

#  write each chunk to a separate file
cat(" Starting jobs sequential\n")
tictoc::tic()

with_progress({
   p <- progressr::progressor(NUM_CHUNKS)
   chunks_combined <- future_walk(1:NUM_CHUNKS, generate_multiple_observations,
                              .options = furrr_options(seed = 123))
})
tictoc::toc()

