# %%%
# name: DummyConcatCombiner
# type: dummy_combiner
# affinity: cpu
# version: 0.0.1
# inputs:
# 	collect_strs:
# outputs:
# 	comb_out:
# desc:
# %%%



source("core/executors/r/transform_base.R")

DummyConcatCombiner <- R6Class(
  classname = "DummyConcatCombiner",
  inherit = TransformBase,
  public = list(
    apply = function() {
      comb_out <- '|'
      for (inp in names(self$inputs[['collect_strs']])) {
        comb_out <- paste0(comb_out, self$inputs[['collect_strs']][[inp]], '|')
      }
      self$outputs <- list(
        "comb_out" = list("value"=comb_out)
      )
    }
  )
)