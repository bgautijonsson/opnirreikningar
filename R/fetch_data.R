#' @export
make_url <- function(from, to) {
  box::use(
    glue[glue]
  )
  from <- format(from, "%d.%m.%Y")
  to <- format(to, "%d.%m.%Y")

  base_url <- paste(
    "https://www.opnirreikningar.is/rest/csvExport?vendor_id",
    "type_id",
    "org_id",
    "",
    sep = "=&"
  )

  glue("{base_url}timabil_fra={from}&timabil_til={to}")
}

#' @export
fetch_data <- function(from, to) {
  box::use(
    readxl[read_excel]
  )

  url <- make_url(from, to)

  tmp <- tempfile()

  download.file(
    url = url,
    dest = tmp
  )

  res <- try(read_excel(tmp))

  if (inherits(res, "try-error")) {
    return(NULL)
  }

  res
}
