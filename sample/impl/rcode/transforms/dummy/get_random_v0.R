# %%%
# name: GetRandomV0
# type: generate_random_number
# affinity: cpu
# version: 0.0.1
# inputs:
# outputs:
# 	new_str:
# desc:
# %%%


source("core/executors/r/transform_base.R")

GetRandomV0 <- R6Class(
  classname = "GetRandomV0",
  inherit = TransformBase,
  public = list(
    apply = function() {
      set.seed(self$params[["seed"]])
      new_str <- toString(sample(1000:9999, 1))
      self$logger$info('test logging')
      self$outputs <- list(
        "new_str"=list("value"=new_str)
      )
    }
  )
)