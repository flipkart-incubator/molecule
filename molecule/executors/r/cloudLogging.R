library(rlang)
library(R6)

cloud_logging <- new_environment(
  data = list(
    log = function(msg, log_name, timestamp, client, user=NULL, deployment_type='prod', severity="INFO") {
      log_url <- 'https://logging.googleapis.com/v2/entries:write'
      log_entry <- list(
        logName = paste('projects', client$project, 'logs', log_name, sep='/'),
        resource = list(
          type = 'gce_instance',
          labels = list(
            instance_id = as.character(client$client_info$id),
            zone = client$client_info$zone
          )
        ),
        entries = list(
          list(
            severity = severity,
            textPayload = msg,
            labels = list(
              log_timestamp = as.character(timestamp),
              r_logger = log_name,
              transform_hash = log_name,
              molecule = 'true',
              deployment_type = deployment_type,
              user = user
            )
          )
        )
      )
      res <- httr::POST(log_url,
                        httr::add_headers('Authorization' = paste('Bearer', client$credentials)),
                        httr::add_headers('Accept' = 'application/json'),
                        httr::add_headers('Content-Type' = 'application/json'),
                        body = log_entry, encode = 'json')
    }
  )
)