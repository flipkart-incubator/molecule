# %%%
# name: JoinToStr
# type: join_list_to_str
# affinity: cpu
# version: 0.0.1
# inputs:
# 	str_list:
# outputs:
# 	joined_str:
# desc:
# %%%


source("core/executors/r/transform_base.R")

JoinToStr <- R6Class(
  classname = "JoinToStr",
  inherit = TransformBase,
  public = list(
    apply = function() {
      str_list <- self$inputs[["str_list"]][["value"]]
      joined_str <- paste(str_list, collapse = "")
      self$outputs <- list(
        "joined_str"=list("value"=joined_str)
      )
    }
  )
)