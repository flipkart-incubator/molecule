library(rlang)
library(R6)

cloud_firestore <- new_environment(
  data = list(
    document_response_parser = function(response_content) {
      fields <- response_content$fields
      data <- list()
      for (field in names(fields)) {
        if ("nullValue" %in% names(fields[[field]]) & is.null(fields[[field]]$nullValue)) {
          data[[field]] = NULL
          next
        } else if (!is.null(fields[[field]]$stringValue)) {
          data[[field]] = as.character(fields[[field]]$stringValue)
          next
        } else if (!is.null(fields[[field]]$integerValue)) {
          data[[field]] = as.integer(fields[[field]]$integerValue)
          next
        } else if (!is.null(fields[[field]]$doubleValue)) {
          data[[field]] = as.double(fields[[field]]$doubleValue)
          next
        } else if (!is.null(fields[[field]]$booleanValue)) {
          data[[field]] = as.logical(fields[[field]]$booleanValue)
          next
        } else if (!is.null(fields[[field]]$timestampValue)) {
          data[[field]] = as.POSIXct(fields[[field]]$timestampValue, tz='UTC')
          next
        } else if (!is.null(fields[[field]]$arrayValue)) {
          data[[field]] = list()
          for (array_value in fields[[field]]$arrayValue$values) {
            if (!is.null(array_value$nullValue)) {
              data[[field]] = c(data[[field]], NULL)
              next
            } else if (!is.null(array_value$stringValue)) {
              data[[field]] = c(data[[field]], as.character(array_value$stringValue))
              next
            } else if (!is.null(array_value$integerValue)) {
              data[[field]] = c(data[[field]], as.integer(array_value$integerValue))
              next
            } else if (!is.null(array_value$doubleValue)) {
              data[[field]] = c(data[[field]], as.double(array_value$doubleValue))
              next
            } else if (!is.null(array_value$booleanValue)) {
              data[[field]] = c(data[[field]], as.logical(array_value$booleanValue))
              next
            } else if (!is.null(array_value$timestampValue)) {
              data[[field]] = c(data[[field]], as.POSIXct(array_value$timestampValue, tz='UTC'))
              next
            } else {
              stop(paste('Failed to parse array value', array_value))
            }
          }
          next
        } else if (!is.null(fields[[field]]$mapValue)) {
          data[[field]] = cloud_firestore$document_response_parser(fields[[field]]$mapValue)
          next
        } else {
          print(fields[[field]])
          stop(paste('Failed to parse field', field))
        }
      }
      return(data)
    },
    document_request_parser = function(data) {
      # FIXME: handle timestampValue
      # "Invalid value at 'document.fields[4].value.timestamp_value' (type.googleapis.com/google.protobuf.Timestamp), Field 'timestampValue', Illegal timestamp format; timestamps must end with 'Z' or have a valid timezone offset."
      fields <- list()
      for (field in names(data)) {
        if (is.null(data[[field]])) {
          fields[[field]] = list(nullValue = NULL)
          next
        } else if (is.character(data[[field]])) {
          fields[[field]] = list(stringValue = data[[field]])
          next
        } else if (is.integer(data[[field]])) {
          fields[[field]] = list(integerValue = data[[field]])
          next
        } else if (inherits(data[[field]], 'POSIXct')) {
          fields[[field]] = list(timestampValue = data[[field]])
          next
        } else if (is.double(data[[field]])) {
          fields[[field]] = list(doubleValue = data[[field]])
          next
        } else if (is.logical(data[[field]])) {
          fields[[field]] = list(booleanValue = data[[field]])
          next
        } else if (is.list(data[[field]])) {
          if (length(data[[field]]) == 0) {
            fields[[field]] = list(arrayValue = list(values = list()))
            next
          }
          values <- list()
          for (value in data[[field]]) {
            if (is.null(value)) {
              values = c(values, list(list(nullValue = NULL)))
              next
            } else if (is.character(value)) {
              values = c(values, list(list(stringValue = value)))
              next
            } else if (is.integer(value)) {
              values = c(values, list(list(integerValue = value)))
              next
            } else if (inherits(value, 'POSIXct')) {
              values = c(values, list(list(timestampValue = value)))
              next
            } else if (is.double(value)) {
              values = c(values, list(list(doubleValue = value)))
              next
            } else if (is.logical(value)) {
              values = c(values, list(list(booleanValue = value)))
              next
            } else {
              stop(paste('Failed to parse array value', value))
            }
          }
          fields[[field]] = list(arrayValue = list(values = values))
          next
        } else if (is.environment(data[[field]])) {
          fields[[field]] = list(mapValue = cloud_firestore$document_request_parser(data[[field]]))
          next
        }
        else {
          stop(paste('Failed to parse field', field))
        }
      }
      return(list(fields = fields))
    },
    get_document = function(collection, document_id, client) {
      url <- paste0(client$api_endpoint, client$project, '/databases/(default)/documents/', collection, '/', document_id)
      res <- httr::GET(url,
                       httr::add_headers('Authorization' = paste('Bearer', client$credentials)),
                       httr::add_headers('Accept' = 'application/json'),
                       httr::add_headers('Content-Type' = 'application/json'))
      if (httr::status_code(res) != 200) {
        # stop(paste('Failed to get document', document_id, 'from collection', collection, 'with status code', httr::status_code(res)))
        return(NULL)
      }
      # return(httr::content(res))
      return(cloud_firestore$document_response_parser(httr::content(res)))
    },
    update_document = function(collection, document_id, data, client) {
      url <- paste0(client$api_endpoint, client$project, '/databases/(default)/documents/', collection, '/', document_id)
      res <- httr::PATCH(url,
                         httr::add_headers('Authorization' = paste('Bearer', client$credentials)),
                         httr::add_headers('Accept' = 'application/json'),
                         httr::add_headers('Content-Type' = 'application/json'),
                         body=jsonlite::toJSON(cloud_firestore$document_request_parser(data), auto_unbox = TRUE))
      return(res)
      if (httr::status_code(res) != 200) {
        # stop(paste('Failed to update document', document_id, 'from collection', collection, 'with status code', httr::status_code(res)))
        return(NULL)
      }
      return(httr::content(res))
    }
  )
)
