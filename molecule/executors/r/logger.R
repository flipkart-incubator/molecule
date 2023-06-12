library(logging)
library(rlang)
library(R6)
source("core/executors/r/gcpClient.R")
source("core/executors/r/cloudLogging.R")

logger <- new_environment(
  data = list(
    getLogger = function (hash_key, user=None, deployment_type='prod', stderr=FALSE, enable_cloud_logging=TRUE) {
      basicConfig(level='FINEST')
      logging::removeHandler('basic.stdout')
      timestamp <- as.integer(as.POSIXct(Sys.time()))
      log <- Logger$new(timestamp, hash_key, user=user, deployment_type=deployment_type, stderr=stderr, enable_cloud_logging=enable_cloud_logging)
      return (log)
    },
    consoleFormatter = function(record) {
      sprintf(paste(record$timestamp, record$levelname, record$msg, sep=' - '))
    },
    cloudLoggingFormatter = function(record) {
      sprintf(record$msg)
    },
    writeToStdErr = function(msg, handler,  ...) {
      if (length(list(...)) && "dry" %in% names(list(...))) {
        handler$color_msg <- function(msg, level_name) msg
        return(TRUE)
      }

      stopifnot(length(list(...)) > 0)

      level_name <- list(...)[[1]]$levelname
      msg <- handler$color_msg(msg, level_name)

      write(paste0(msg, " "), stderr())
    },
    writeToCloudLogging = function(msg, handler, ...) {
      if (length(list(...)) && "dry" %in% names(list(...))) {
        handler$color_msg <- function(msg, level_name) msg
        return(TRUE)
      }

      stopifnot(length(list(...)) > 0)

      level_name <- list(...)[[1]]$levelname
      hash_key <- list(...)[[1]]$logger
      timestamp <- with(handler, timestamp)
      client <- with(handler, client)
      user <- with(handler, user)
      deployment_type <- with(handler, deployment_type)

      cloud_logging$log(msg, hash_key, timestamp, client, user=user, deployment_type=deployment_type, severity=level_name)
    }
  )
)

Logger <- R6Class(
  "Logger",
  public = list(
    hash_key = NULL,
    cloud_client = NULL,
    initialize = function (timestamp, hash_key, user=user, deployment_type=deployment_type, stderr=F, enable_cloud_logging=T) {
      self$hash_key <- hash_key
      logging::getLogger(hash_key)
      if (!stderr){
        addHandler(writeToConsole, level='FINEST', formatter = logger$consoleFormatter, logger = hash_key)
      } else {
        addHandler(logger$writeToStdErr, level='FINEST', formatter = logger$consoleFormatter, logger = hash_key)
      }
      if (enable_cloud_logging) {
        self$cloud_client <- GCPClient$new()
        addHandler(logger$writeToCloudLogging, level='FINEST', formatter = logger$cloudLoggingFormatter, 
                  timestamp=timestamp, client=self$cloud_client, user=user, deployment_type=deployment_type, logger = hash_key)
      }
    },
    debug= function (message) {
      if (typeof(message) == "character") {
        message <- capture.output(cat(message))
      } else {
        message <- paste(capture.output(print(message)), collapse = '\n')
      }
      logging::logdebug(message, logger = self$hash_key)
    },
    info = function (message) {
      if (typeof(message) == "character") {
        message <- capture.output(cat(message))
      } else {
        message <- paste(capture.output(print(message)), collapse = '\n')
      }
      logging::loginfo(message, logger = self$hash_key)
    },
    warning = function (message) {
      if (typeof(message) == "character") {
        message <- capture.output(cat(message))
      } else {
        message <- paste(capture.output(print(message)), collapse = '\n')
      }
      logging::logwarn(message, logger = self$hash_key)
    },
    critical = function (message) {
      if (typeof(message) == "character") {
        message <- capture.output(cat(message))
      } else {
        message <- paste(capture.output(print(message)), collapse = '\n')
      }
      logging::logerror(message, logger = self$hash_key)
    }
  )
)