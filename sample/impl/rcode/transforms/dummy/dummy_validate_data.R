# %%%
# name: DummyValidateData
# type: dummy_data_import
# affinity: cpu
# version: 0.0.1
# inputs:
#   hive_raw:
#     hive_raw:
# outputs:
#   validated_data:
#     data:
# desc:
# %%%



source("core/executors/r/transform_base.R")

DummyValidateData <- R6Class(
  classname = "DummyValidateData",
  inherit = TransformBase,
  public = list(
    apply = function() {
      hive_raw <- self$inputs[["hive_raw"]][["hive_raw"]]
      validated_data <- NULL
      delete_hashes <- NULL
      if (unique(hive_raw$super_category) == self$params[["sc"]]) {
        validated_data <- hive_raw
      }
      else {
        delete_hashes <- c("hive_raw")
      }
      self$outputs <- list(
        "validated_data"=list("data"=validated_data, "sc"=self$params[["sc"]]),
        "delete_hashes"=delete_hashes
      )
    }
  )
)
