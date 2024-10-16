library(tidyverse)
library(arrow)

read_parquet("data/year=2017/month=8/part-0.parquet")


d <- open_dataset("data") |>
  to_duckdb()

d

d |>
  collect()


d |>
  summarise(
    n = n(),
    total_upphaed = sum(upphaed_linu, na.rm = TRUE),
    .by = kaupandi
  ) |>
  arrange(desc(n)) |>
  collect()
