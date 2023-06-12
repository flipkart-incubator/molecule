library(yaml)
library(readr)
library(data.table)
# library(feather)
library(R6)

SerDe <- R6Class(
  classname = "SerDe",
  public = list(
    data_defs = list(),
    initialize = function (defs_path) {
      data_def_files <- list.files(defs_path, full.names = T)
      for (def_file_name in data_def_files) {
        defs <- yaml.load_file(def_file_name)
        self$data_defs <- c(self$data_defs, defs)
      }
    },
    read = function (file_path, file_type) {
      blob <- list()
      kv_pairs <- NULL

      for (var_name in names(self$data_defs[[file_type]])) {
        if (self$data_defs[[file_type]][[var_name]] == 'df') {
          blob[[var_name]] <- self$read_df(file_path, var_name)
        } else {
          loc <- file.path(file_path, 'key_value.yaml')
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
      return (blob)
    },
    write = function (file_path, blob, file_type, ttl=NULL) {
      dir.create(file_path, showWarnings = F)
      if (!is.null(ttl)) {
        ttl_loc = file.path(file_path, paste0('del_', ttl, '.ttl'))
        write('', ttl_loc)
      }
      kv_pairs <- list()
      for (var_name in names(self$data_defs[[file_type]])) {
        if (self$data_defs[[file_type]][[var_name]] == 'df') {
          self$write_df(file_path, blob[[var_name]], var_name)
        } else {
          kv_pairs[[var_name]] <- blob[[var_name]]
        }
      }
      if (length(kv_pairs)) {
        loc <- file.path(file_path, 'key_value.yaml')
        write_lock <- file.path(file_path, 'write.lock')
        write('', write_lock)
        write_yaml(kv_pairs, loc)
        file.remove(write_lock)
      }
    },
    read_df = function(file_path, file_name) {
      loc <- file.path(file_path, paste0(file_name, '.rds'))
      if (file.exists(loc)) {
        blob <- readRDS(loc)
        return (blob)
      }
      # loc <- file.path(file_path, paste0(file_name, '.feather'))
      # if (file.exists(loc)) {
      #   blob <- read_feather(loc)
      #   return (blob)
      # }
      loc <- file.path(file_path, paste0(file_name, '.csv'))
      if (file.exists(loc)) {
        blob <- fread(loc, data.table = FALSE)
        return (blob)
      }
      loc <- file.path(file_path, paste0(file_name, '.tsv'))
      if (file.exists(loc)) {
        blob <- fread(loc, data.table = FALSE)
        return (blob)
      } else {
        stop("file not found: ", loc)
      }
    },
    write_df = function (file_path, blob, file_name) {
      if (is.null(blob)) {
        stop("got NULL in dataframe")
      }
      loc <- file.path(file_path, paste0(file_name, '.rds'))
      write_lock <- file.path(file_path, 'write.lock')
      write('', write_lock)
      saveRDS(blob, loc)
      # loc <- file.path(file_path, paste0(file_name, '.feather'))
      # write_feather(blob, loc)
      loc <- file.path(file_path, paste0(file_name, '.csv'))
      fwrite(blob, loc)
      file.remove(write_lock)
    }
  )
)
