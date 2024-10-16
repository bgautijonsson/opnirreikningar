box::use(
  arrow[write_parquet],
  clock[date_build],
  dplyr[filter, mutate],
  glue[glue],
  janitor[clean_names],
  lubridate[days, month, year],
  purrr[map2],
  tibble[tibble],
  tidyr[crossing],
)

box::use(
  R / fetch_data[fetch_data]
)
# Initiate the dates
dates <- crossing(
  year = 2017:2024,
  month = 1:12
) |>
  mutate(
    start_date = date_build(year, month, 1),
    end_date = start_date + months(1) - days(1)
  ) |>
  filter(start_date >= date_build(2017, 7, 1))


# Fetch the data
map2(
  dates$start_date,
  dates$end_date,
  function(start_date, end_date) {
    Sys.sleep(0.1)
    cur_year <- year(start_date)
    cur_month <- month(start_date)

    # Create the file name
    file_name <- glue("data/year={cur_year}/month={cur_month}/part-0.parquet")

    # Create the directory if it doesn't exist
    if (!dir.exists(glue("data/year={cur_year}"))) {
      dir.create(glue("data/year={cur_year}"), recursive = TRUE)
    }

    # Create the directory if it doesn't exist
    if (!dir.exists(glue("data/year={cur_year}/month={cur_month}"))) {
      dir.create(glue("data/year={cur_year}/month={cur_month}"), recursive = TRUE)
    }

    # Fetch the data and write to parquet
    result <- fetch_data(start_date, end_date)

    if (!is.null(result)) {
      result |>
        clean_names() |>
        write_parquet(file_name)
    }
  }
)
