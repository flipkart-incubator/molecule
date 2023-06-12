library(R6)
library(yaml)
library(filelock)
library(digest)
library(stringr)
source("core/executors/r/serde.R")
source("core/executors/r/gcs_serde.R")
source("core/executors/r/gcpClient.R")

if (Sys.getenv('MONGO_URI') != '') {
  MONGO_URI <- Sys.getenv('MONGO_URI')
} else {
  MONGO_URI <- 'mongodb://mongo0:27017,mongo1:27017/'
}

BaseStorage <- R6Class(
  "BaseStorage",
  public = list(
    mode = 'nfs',
    client = NULL,
    store_loc = '',
    pl_plan_store = '',
    ti_plan_store = '',
    data_store = '',
    memory_store = list(),
    initialize = function(store_loc, mkdir=TRUE, mode='nfs') {
      self$mode = mode
      self$store_loc = store_loc
      if (mkdir) {
        dir.create(self$store_loc, recursive = TRUE, showWarnings = FALSE)
      }
      self$data_store = file.path(self$store_loc, 'data')
      if (mkdir && self$mode == 'nfs') {
        dir.create(self$data_store, recursive = TRUE, showWarnings = FALSE)
      }
    },
    check = function(hash) {
      stop("check not implemented in class")
    },
    load = function(hash, blob_type) {
      stop("load not implemented in class")
    },
    save = function(hash, blob) {
      stop("save not implemented in class")
    },
    saveToFile = function(hash, blob, blob_type, ttl=28) {
      stop("saveToFile not implemented in class")
    },
    delete = function(hash) {
      stop("delete not implemented in class")
    }
  )
)

Storage <- R6Class(
  "Storage",
  inherit = BaseStorage,
  public = list(
    serde = NULL,
    request_id = '0000000',
    partition_val = NULL,
    cloud_client = NULL,
    remote_loc=NULL,
    db_env='stage',
    datasets=NULL,
    user=NULL,
    initialize = function(store_loc, defs_path, mode='nfs', db=NULL, remote_loc=NULL, db_env='stage') {
      super$initialize(store_loc, mode=mode)
      self$db_env <- db_env
      if (self$mode == 'nfs') {
        self$serde = SerDe$new(defs_path)
      }
      if (self$mode == 'gcs') {
        self$serde = GCSSerDe$new(defs_path)
        # self$cloud_client <- GCPClient$new()
        self$cloud_client = db
        self$remote_loc = remote_loc
        self$datasets <- mongolite::mongo(collection = 'datasets', db = db_env, url = MONGO_URI)
        # print(remote_loc)
        #user <- tail(strsplit(strsplit(remote_loc, split='/store/data')[[1]], split='/')[[1]], n=1)
        #self$user <- str_replace(user, '[.]', '')
      }
    },
    check = function(hash) {
      if (hash %in% names(self$memory_store)) {
        return (TRUE)
      }
      # FIXME: not actually looking for file, might be an error
      loc <- file.path(self$data_store, hash)
      write_lock <- file.path(loc, 'write.lock')
      if (self$mode == 'nfs') {
        return(dir.exists(loc) && !file.exists(write_lock))
      }
      if (self$mode == 'gcs') {
        if (dir.exists(loc) && !file.exists(write_lock)) {
          return(T)
        } else {
          # Checking in load function to get TTL and returing TRUE here to avoid multiple db calls
          return(T)
          # return(self$serde$gsutil_helper(loc, 'check') && !self$serde$gsutil_helper(write_lock, 'check'))
        }
      }
    },
    load = function(hash, blob_type) {
      if (self$check(hash)) {
        if (hash %in% names(self$memory_store)) {
          return (self$memory_store[[hash]])
        }
        if (self$mode == 'gcs'){
          ## remote loc doesn't end with / so split key is different
          hash_key <- paste0(hash, "_", self$get_user())
          t_hash_dict <- self$datasets$find(query = paste0('{"_id":"', hash_key, '"}'))
          if (is.null(t_hash_dict)){
            stop(paste0('No such hash in store. ', hash))
          }
          ds_ttl <- t_hash_dict$ttl
          loc <- file.path(self$data_store, hash)
          return(self$serde$read(loc, blob_type, self$remote_loc, ds_ttl))
        }
        else{
          loc <- file.path(self$data_store, hash)
          return(self$serde$read(loc, blob_type))
        }
        
      }
      stop(paste0('No such hash in store. ', hash))
    },
    save = function(hash, blob) {
      self$memory_store[[hash]] = blob
    },
    # TODO: Disabled for 
    saveToFile = function(hash, blob, blob_type, ttl=28) {
      self$save(hash, blob)
      loc <- file.path(self$data_store, hash)
      if (self$mode == 'gcs'){
        self$serde$write(loc, blob, blob_type, self$remote_loc, ttl)
        self$update_dataset_status(hash, DBStatus$COMPLETED)
      }
      else{
        self$serde$write(loc, blob, blob_type, ttl)
      }
    },
    get_user = function(){
      user <- tail(strsplit(strsplit(self$remote_loc, split='/store/data')[[1]], split='/')[[1]], n=1)
      user <- str_replace(user, '[.]', '')
      return(user)
    },
    update_dataset_status = function(hash, status){      
      hash_key <- paste0(hash, "_", self$get_user())
      set_dict = list()
      set_dict[['status']] = status
      del_status <- self$datasets$update(query = paste0('{"_id":"', hash_key, '"}'), 
                                    update = paste0('{"$set":', jsonlite::toJSON(set_dict, auto_unbox=T), '}'))
    },

    delete = function(hash) {
      if (self$check(hash)) {
        loc <- file.path(self$data_store, hash)
        self$update_dataset_status(hash, DBStatus$STALE)
        if (self$mode == 'nfs') {
          unlink(loc, recursive = T, force = T)
        }
        if (self$mode == 'gcs') {
          self$serde$gsutil_helper(loc, self$remote_loc, 'delete')
        }
      }
    }
  )
)

DebugStorage <- R6Class(
  "DebugStorage",
  inherit = BaseStorage,
  public = list(
    remote_ip = NULL,
    local_store = NULL,
    remote_store = NULL,
    initialize = function (store_loc, remote_ip, remote_store_loc, defs_path, mode='nfs', db=NULL, db_env='stage') {
      super$initialize(store_loc, mode=mode)
      self$remote_ip = remote_ip
      self$local_store = Storage$new(store_loc, defs_path, mode=mode, db=db, db_env=db_env, remote_loc=remote_store_loc)
      if (is.null(remote_store_loc)) {
        remote_store_loc <- '/tmp'
      }
      if (is.null(self$remote_ip)) {
        self$remote_store = Storage$new(remote_store_loc, defs_path, mode=mode)
      } else {
        self$remote_store = BaseStorage$new(remote_store_loc, mkdir=FALSE)
      }
    },
    # loadTransform = function (ti_hash) {
    #   return (self$local_store$loadTransform(ti_hash))
    # },
    remoteCheck = function (remote_loc) {
      cmd <- paste0('ssh -o ConnectTimeout=5 ', self$remote_ip, ' test -e ', remote_loc)
      p <- system(cmd, intern = F)
      remote_write_lock <- file.path(remote_loc, 'write.lock')
      cmd <- paste0('ssh -o ConnectTimeout=5 ', self$remote_ip, ' test -e ', remote_write_lock)
      q <- system(cmd, intern = F)
      if (p == 0 && q != 0) {
        return (T)
      } else {
        return (F)
      }
    },
    remoteCopy = function (remote_loc) {
      cmd <- paste0('rsync -e "ssh -o ConnectTimeout=5" --info=progress2 -rzh ', self$remote_ip, ':', remote_loc, ' ', self$local_store$data_store)
      p <- system(cmd, intern = F)
      if (p != 0) {
        stop("SCP file transfer error")
      } else {
        return (TRUE)
      }
    },
    check = function (hash) {
      if (self$local_store$check(hash)) {
        return (TRUE)
      }
      if (is.null(self$remote_ip)) {
        return (self$remote_store$check(hash))
      } else {
        remote_loc <- file.path(self$remote_store$data_store, hash)
        return (self$remoteCheck(remote_loc))
      }
    },
    load = function (hash, blob_type) {
      if (self$check(hash)) {
        if (self$local_store$check(hash)) {
          fl <- filelock::lock(file.path('/tmp/', hash), timeout = 0)
          if (is.null(fl)) {
            stop("BlockingIOError: Resource temporarily unavailable")
          }
          blob <- self$local_store$load(hash, blob_type)
          unlock(fl)
          fl <- NULL
          return (blob)
        } else if (is.null(self$remote_ip) & self$remote_store$check(hash)) {
          fl <- filelock::lock(file.path('/tmp/', hash), timeout = 0)
          if (is.null(fl)) {
            stop("BlockingIOError: Resource temporarily unavailable")
          }
          blob <- self$remote_store$load(hash, blob_type)
          unlock(fl)
          fl <- NULL
          return (blob)
        } else {
          remote_loc <- file.path(self$remote_store$data_store, hash)
          fl <- filelock::lock(file.path('/tmp/', hash), timeout = 0)
          if (is.null(fl)) {
            stop("BlockingIOError: Resource temporarily unavailable")
          }
          if (self$remoteCopy(remote_loc)) {
            unlock(fl)
            fl <- NULL
            return (self$local_store$load(hash, blob_type))
          }
        }
      }
      stop(paste0("No such hash in store: ", hash))
    },
    save = function (hash, blob) {
      self$local_store$save(hash, blob)
    },
    saveToFile = function (hash, blob, blob_type, ttl=28) {
      self$local_store$saveToFile(hash, blob, blob_type, ttl)
    },
    delete = function (hash) {
      self$local_store$delete(hash)
    }
  )
)