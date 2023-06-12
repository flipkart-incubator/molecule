library(R6)
library(logging)

TransformBase <- R6Class(
    classname = "TransformBase",
    public = list(
      inputs = NA,
      params = NA,
      outputs = NA,
      logger = NA,
      initialize = function(inputs, params, outputs = list(), context = list()) {
        self$inputs <- inputs
        self$params <- params
        self$outputs <- outputs
        if (is.null(context$logger)) {
          logging::basicConfig(level = 'FINEST')
          self$logger <- list(
            debug = logging::logdebug,
            info = logging::loginfo,
            warning = logging::logwarn,
            critical = logging::logerror
          )
        } else {
          self$logger <- context$logger
        }
      },
      apply = function() {
        stop("sub class doesn't support apply call")
      },
      run = function () {
        tryCatch(
          {
            self$apply()
          },
          error=function(cond) {
            self$logger$critical(paste0(class(self)[1], " failed to apply, please check and run again"))
            self$logger$critical(cond)
            quit(save = "no", status = 1)
          }
        )
      }
    )
)
