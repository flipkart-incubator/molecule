# %%%
# name: MakeDummyList
# type: make_dummy_list
# affinity: cpu
# version: 0.0.1
# inputs:
# 	cut_str:
# outputs:
# 	str_list:
# desc:
# %%%


source("core/executors/r/transform_base.R")

MakeDummyList <- R6Class(
  classname = "MakeDummyList",
  inherit = TransformBase,
  public = list(
    apply = function() {
      cut_str <- self$inputs[["cut_str"]][["value"]]
      n_repeat <- self$params[["repeat"]]
      str_list <- rep(cut_str, each=n_repeat)
      self$outputs <- list(
        "str_list" = list("value"=str_list)
      )
    }
  )
)