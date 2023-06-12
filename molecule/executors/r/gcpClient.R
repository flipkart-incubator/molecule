library(rlang)
library(R6)

cloud_metadata <- new_environment(
  data = list(
    getInstanceMetadata = function(metadata_url=NULL) {
      if (is.null(metadata_url)) {
        metadata_url <- 'http://metadata.google.internal/computeMetadata/v1/instance/'
      }
      required_meta <- c('id', 'name', 'zone', 'hostname')
      res <- httr::GET(paste0(metadata_url, '?recursive=true'), httr::add_headers('Metadata-Flavor' = 'Google'))
      if (httr::status_code(res) != 200) {
        stop('Failed to get instance metadata, you might not be working on GCP, ask moderators for support.')
      }
      metadata <- httr::content(res)
      metadata <- metadata[required_meta]
      # Avoid integer coercion
      metadata['id'] <- httr::content(httr::GET(paste0(metadata_url, 'id'), 
                                      httr::add_headers('Metadata-Flavor' = 'Google')), as='text', encoding='UTF-8')
      return(metadata)
    },
    getGcloudProject = function() {
      res <- system('gcloud config get-value project', intern = T)
      if (length(res) == 0) {
        stop('No gcloud project found. Please run `gcloud config set project <project_id>`')
      }
      return(res)
    },
    getDBEndpoint = function(){
      endpoint <- Sys.getenv('FIRESTORE_EMULATOR_HOST')
      if (endpoint == "" ){
        api_endpoint <- 'https://firestore.googleapis.com/v1/projects/'
      }
      else{
        api_endpoint <- paste0("http://", endpoint, "/emulator/v1/projects/")
      }
      return(api_endpoint)
    },
    getGcloudAuthToken = function() {
      res <- system('gcloud auth print-access-token', intern = T)
      if (length(res) == 0) {
        stop('No gcloud auth token found. Please run `gcloud auth login`')
      }
      return(res)
    }
  )
)

DBStatus <- new_environment(
  data = list(
    QUEUED = 0,
    PROCESSING = 1,
    COMPLETED = 2,
    TERMINATED = 3,
    STALE = 4
  )

)

GCPClient = R6Class(
  "GCPClient",
  lock_class = FALSE,
  lock_objects = FALSE,
  public = list(
    project = NULL,
    credentials = NULL,
    client_info = NULL,
    api_endpoint = NULL,
    initialize = function (project=NULL, credentials=NULL, client_info=NULL, api_endpoint=NULL) {
      if (is.null(project)) {
        self$project <- cloud_metadata$getGcloudProject()
      } else {
        self$project <- project
      }
      if (is.null(credentials)) {
        self$credentials <- cloud_metadata$getGcloudAuthToken()
      } else {
        self$credentials <- credentials
      }
      if (is.null(client_info)) {
        self$client_info <- cloud_metadata$getInstanceMetadata()
      } else {
        self$client_info <- client_info
      }
      if (is.null(api_endpoint)) {
        self$api_endpoint <- cloud_metadata$getDBEndpoint()
      } else {
        self$api_endpoint <- api_endpoint
      }
    }
  )
)

