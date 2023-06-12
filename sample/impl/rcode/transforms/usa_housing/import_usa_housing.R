# %%%
# name: ImportUSAHousing
# type: import_usa_housing
# affinity: cpu
# version: 0.3.0
# inputs: 
#   in_var: 
#     hive_raw: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price, address
# outputs: 
#   out_var: 
#     data: avg_area_income, avg_area_house_age, avg_area_no_of_rooms, avg_area_no_of_bedrooms, area_popluation, price, address
# desc: 
# %%%

source("core/executors/r/transform_base.R")

ImportUSAHousing <- R6Class(
  classname = "ImportUSAHousing",
  inherit = TransformBase,
  public = list(
    apply = function() {
      check_no_of_cols <- self$params[['check_no_of_cols']]
      df <- self$inputs[["in_var"]][["hive_raw"]]
      if ('refresh_id' %in% names(df)) {
        df$refresh_id <- NULL
      }
      validated_data <- NULL

      df <- df %>%
        filter(!is.na(price))

      if (nrow(df)>0 & ncol(df)==check_no_of_cols) {
        validated_data <- df
      }
      else {
        stop("Invalid data!")
      }

      self$logger$info('Dataframe Summary')
      self$logger$info(summary(validated_data))

      self$outputs <- list(
        "out_var"=list(
          "data"=validated_data, 
          "row_count"=nrow(df)
        )
      )
    }
  )
)
