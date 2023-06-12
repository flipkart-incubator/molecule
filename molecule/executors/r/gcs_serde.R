library(yaml)
library(readr)
library(data.table)
# library(feather)
library(R6)

GCSSerDe <- R6Class(
  classname = "GCSSerDe",
  public = list(
    data_defs = list(),
    initialize = function (defs_path) {
      data_def_files <- list.files(defs_path, full.names = T)
      for (def_file_name in data_def_files) {
        defs <- yaml.load_file(def_file_name)
        self$data_defs <- c(self$data_defs, defs)
      }
    },
    gsutil_helper = function (file_path, remote_loc, mode) {
      p <- system(paste("python3 core/gsutil_helper.py -fp", file_path, "-rp", remote_loc, "-m", mode, sep=" "))
      if (p == 0) {
        return(T)
      } else {
        return(F)
      }
    },
    read = function (file_path, file_type, remote_loc, ttl=14) {
      if (!dir.exists(file_path) && !self$gsutil_helper(file_path, remote_loc, 'read')) {
        stop('gcs copy failed')
      }
      blob <- list()
      kv_pairs <- NULL

      for (var_name in names(self$data_defs[[file_type]])) {
        if (self$data_defs[[file_type]][[var_name]] == 'df') {
          blob[[var_name]] <- self$read_df(file_path, var_name, ttl)
        } else {
          loc <- file.path(file_path, paste0('key_value.yaml.ttl', ttl))
          if (file.exists(loc)) {
            kv_pairs <- yaml.load_file(loc)
          } else {
            stop("file not found: ", loc)
          }
        }
      }
      for (key in names(kv_pairs)) {
        blob[[key]] <- kv_pairs[[key]]
      }
      unlink(file_path, recursive=T)

      return (blob)
    },
    write = function (file_path, blob, file_type, remote_loc, ttl=NULL) {
      dir.create(file_path, showWarnings = F)
      
      # if (!is.null(ttl)) {
      #   ttl_loc = file.path(file_path, paste0('del_', ttl, '.ttl'))
      #   write('', ttl_loc)
      # }

      kv_pairs <- list()
      for (var_name in names(self$data_defs[[file_type]])) {
        if (self$data_defs[[file_type]][[var_name]] == 'df') {
          self$write_df(file_path, blob[[var_name]], var_name, ttl)
        } else {
          kv_pairs[[var_name]] <- blob[[var_name]]
        }
      }
      if (length(kv_pairs)) {
        loc <- file.path(file_path, paste0('key_value.yaml.ttl', ttl))
        # write_lock <- file.path(file_path, 'write.lock')
        # write('', write_lock)
        write_yaml(kv_pairs, loc)
        # file.remove(write_lock)
      }
      # if not gsutil_helper(file_path, mode='write'):
      #   shutil.rmtree(file_path)
      #   raise Exception('gcs copy failed')
      if (!self$gsutil_helper(file_path, remote_loc, 'write')) {
        unlink(file_path, recursive=T)
        stop('gcs copy failed')
      }
      unlink(file_path, recursive=T)
    },
    read_df = function(file_path, file_name, ttl=14) {
      loc <- file.path(file_path, paste0(file_name, '.rds.ttl', ttl))
      if (file.exists(loc)) {
        blob <- readRDS(loc)
        return (blob)
      }
      # loc <- file.path(file_path, paste0(file_name, '.feather'))
      # if (file.exists(loc)) {
      #   blob <- read_feather(loc)
      #   return (blob)
      # }
      loc <- file.path(file_path, paste0(file_name, '.csv.ttl', ttl))
      print(loc)
      if (file.exists(loc)) {
        blob <- fread(loc, data.table = FALSE)
        return (blob)
      }
      loc <- file.path(file_path, paste0(file_name, '.tsv.ttl', ttl))
      if (file.exists(loc)) {
        blob <- fread(loc, data.table = FALSE)
        return (blob)
      } else {
        stop("file not found: ", loc)
      }
    },
    write_df = function (file_path, blob, file_name, ttl=14) {
      if (is.null(blob)) {
        stop("got NULL in dataframe")
      }
      loc <- file.path(file_path, paste0(file_name, '.rds.ttl', ttl))
      # write_lock <- file.path(file_path, 'write.lock')
      # write('', write_lock)
      saveRDS(blob, loc)
      # loc <- file.path(file_path, paste0(file_name, '.feather'))
      # write_feather(blob, loc)
      loc <- file.path(file_path, paste0(file_name, '.csv.ttl', ttl))
      fwrite(blob, loc)
      # file.remove(write_lock)
    }
  )
)
