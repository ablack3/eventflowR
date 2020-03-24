# data <- eventflow_setup
# path <- here("test.txt")
# event_attribute_cols <- c("blooddrawwho", "flushtype")

write_eventflow <- function(data,
                            path,
                            record_id_col = colnames(data)[1],
                            event_category_col = colnames(data)[2],
                            start_datetime_col = colnames(data)[3],
                            end_datetime_col = colnames(data)[4],
                            record_attribute_cols = NA_character_,
                            event_attribute_cols = NA_character_,
                            convert_to_datetime = F, debug = F){

  require(dplyr)
  if(debug) browser() # for debugging
  df <- data
  col_names <- c(record_id_col, event_category_col, start_datetime_col, end_datetime_col, record_attribute_cols, event_attribute_cols) %>%
    {.[!is.na(.)]} # remove missing values
  # print(col_names)
  # print(names(df))

  # check the supplied arguments
  if(!(grepl("txt$", path) & is.character(path))) stop("path should be a character string ending with .txt")
  stopifnot(is.data.frame(df), is.character(col_names), is.character(record_attribute_cols), is.character(event_attribute_cols))
  if(!all(col_names %in% names(df))) stop("Not all the supplied column names are in the dataframe")
  if(sum(complete.cases(df[,col_names[1:3]])) != nrow(df)) stop("id, event_type, and start_date columns cannot contain missing data")



  # need to check that each event type is either a point event or a duration
  event_categories <- unique(df[, event_category_col][[1]])
  check <- df %>%
    mutate(record_type_ = ifelse(is.na(end_date), "point", "duration")) %>%
    count(!!as.name(event_category_col), record_type_) %>%
    # is there one row per record type?
    {nrow(.) == length(event_categories)}
  if(!check) stop("Each event category must be either a point (start date/time only) or a duration (both start and end date/time)")

  start <- df[ ,start_datetime_col][[1]]
  end   <- df[ ,end_datetime_col][[1]]


  # check that both start and end datetimes are date or datetime objects
  if(!(lubridate::is.timepoint(start) & lubridate::is.timepoint(end))) stop("Both the start and end date/times must be dates or datetimes")

  # check that dates are supplied when convert_to_datetime is True
  # lubridate::is.Date(lubridate::ymd_h("2018-12-01 9")) # just verify that this test works.
  if(convert_to_datetime & (!lubridate::is.Date(start) | !lubridate::is.Date(end))) stop("If convert_to_datetime is TRUE then dates (without times) must be supplied.")

  # need to check that start date is before (and not equal to) end date
  if(convert_to_datetime == F){
    if(!(all(start[!is.na(end)] < end[!is.na(end)])))
      stop("Start date/time needs to be before end date/time.
           If an event happened on a single day then it should be a point event or times should be included or convert_to_datetime should be set to TRUE.
           Setting convert_to_datetime argument to TRUE to add default times to date values")
  } else {
    if(!(all(start[!is.na(end)] <= end[!is.na(end)])))
      stop("Start date/time needs to be before or on the same day as end date/time.")
  }

  # convert datetimes
  if(convert_to_datetime){
    # if end is NA then leave it alone, otherwise if end is the same day as start then event should end at 5pm,
    # otherwise end should be at least one day after start and should end at 7 am
    for(i in seq_along(start)){
      if(is.na(end[i])){
        # if end is NA then we are dealing with a point event and not a duration. We will say all point events will happen at noon.
        end[i] <- lubridate::ymd_h(paste(as.character(end[i]), "12"))
      } else if(start[i] == end[i]){
        # if start and end dates are the same then the event will start at 8am and end at 5pm
        start[i] <- lubridate::ymd_h(paste(as.character(start[i]), "8"))
        end[i] <- lubridate::ymd_h(paste(as.character(end[i]), "17"))
      } else {
        # we are dealing with a duration where the start and end dates are not the same. These will start at 8am and end at 7am on a subsequent day.
        # In the case where an a
        start[i] <- lubridate::ymd_h(paste(as.character(start[i]), "8"))
        end[i] <- lubridate::ymd_h(paste(as.character(end[i]), "7"))
      }

    }
    end <- lubridate::ymd_h(ifelse(is.na(end), NA, paste(as.character(end), ifelse(start == end, 17, 7))))
    start <- lubridate::ymd_h(paste(as.character(start), 8)) # events start at 8am
  }

  # convert datetimes to character
  # df[,start_datetime_col] <- as.character(df[, start_datetime_col][[1]])
  # df[,  end_datetime_col] <- as.character(df[, end_datetime_col][[1]])
  df[,start_datetime_col] <- as.character(start)
  df[,  end_datetime_col] <- as.character(end)


  # create attribute file

  if(!any(is.na(record_attribute_cols)) & length(record_attribute_cols) > 0){

    df_attrib <- df[,c(record_id_col, record_attribute_cols)] %>%
      dplyr::mutate_at(vars(-1), as.character) %>%
      tidyr::gather("key", "val", -1) %>%
      # make sure there is only one value per person by summarizing to the record id level
      filter(!is.na(val)) %>%
      group_by(!!as.name(record_id_col), key) %>%
      summarise(val = max(val, na.rm = T))

    readr::write_tsv(df_attrib, path = sub("txt$", "attrib", path), col_names = F, na = "", quote_escape = F)
  }


  export_col_names <- c(record_id_col, event_category_col, start_datetime_col, end_datetime_col)

  # if we have event attributes we need to create the event attribute column
  if(!any(is.na(event_attribute_cols)) & length(event_attribute_cols) > 0){

    df_event_attrib <- df %>%
      as_tibble() %>%
      select(!!event_attribute_cols) %>%
      mutate_all(as.character) %>%
      mutate(row_number = row_number()) %>%
      tidyr::gather("key","val", -row_number) %>%
      filter(!is.na(val)) %>%
      mutate(event_attributes = paste0(key, '="', val, '"')) %>%
      group_by(row_number) %>%
      summarise(event_attributes = paste0(event_attributes, collapse = ";")) %>%
      arrange(row_number) %>%
      select(row_number, event_attributes)


    df <- df %>%
      mutate(row_number = row_number()) %>%
      left_join(df_event_attrib, by = "row_number") %>%
      select(-row_number)

    export_col_names <- c(record_id_col, event_category_col, start_datetime_col, end_datetime_col, "event_attributes")
  }

  readr::write_tsv(df[,export_col_names], path = path, col_names = F, na = " ", quote_escape = F)
}

# library(tidyverse)
# mtcars %>%
#   head() %>%
#   as_tibble() %>%
#   mutate_all(as.character) %>%
#   mutate(row_number = row_number()) %>%
#   tidyr::gather("key","val", -row_number) %>%
#   mutate(text = paste0(key, '="', val, '"')) %>%
#   group_by(row_number) %>%
#   summarise(text = paste0(text, collapse = ";")) %>%
#   arrange(row_number) %>%
#   select(text)

