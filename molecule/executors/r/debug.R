library(yaml)
library(rlang)
library(R6)

storage <- new.env()
executor <- new.env()
source("core/executors/r/storage.R", local = storage)
source("core/executors/r/executor.R", local = executor)
source("core/executors/r/logger.R")
source("core/executors/r/gcpClient.R")

if (Sys.getenv('MONGO_URI') != '') {
  MONGO_URI <- Sys.getenv('MONGO_URI')
} else {
  MONGO_URI <- 'mongodb://mongo0:27017,mongo1:27017/'
}

debug <- new_environment(
  data=list(
    IMPORT_FLAG = FALSE,
    store_loc = NULL,
    remote_ip = NULL,
    remote_store_loc = NULL,
    defs_path = NULL,
    store = NULL,
    tasks=NULL,
    reimport = function () {
      executor$importAll()
    },
    setOptions = function (store_loc, remote_ip, remote_store_loc, defs_path, mode='gcs', db_env='stage') {
      debug$store_loc <- store_loc
      debug$remote_ip <- remote_ip
      debug$remote_store_loc <- remote_store_loc
      debug$defs_path <- defs_path
      cloud_client <- GCPClient$new()
      debug$tasks <- mongolite::mongo(collection = 'tasks', db = db_env, url = MONGO_URI, verbose = TRUE)
      debug$store <- storage$DebugStorage$new(store_loc, remote_ip, remote_store_loc, defs_path, 
        mode=mode, db=cloud_client, db_env=db_env)
    },
    generatePlan = function (pipeline_name,  pipeline_spec_path, pipeline_config_path, debug=NULL, expand=FALSE, static_names=FALSE) {
      pl_spec_files <- list.files(pipeline_spec_path, pattern = '*\\.yaml$', full.names = T)
      pl_specs <- list()
      for (pl_spec_loc in pl_spec_files) {
        pl_spec <- yaml.load_file(pl_spec_loc)
        pl_specs <- c(pl_specs, pl_spec)
      }
      pl_tasks <- unlist(names(pl_specs[[pipeline_name]]$tasks))
      if (is.null(debug) || debug == 'local') {
        cmd <- paste0('python3 core/plan.py -pn ', pipeline_name, ' -ps ', pipeline_spec_path, ' -pc ', pipeline_config_path)
      } else {
        cmd <- paste0('python3 core/plan.py -pn ', pipeline_name, ' -ps ', pipeline_spec_path, ' -pc ', pipeline_config_path, ' -d ', debug)
      }
      p <- system(cmd, intern = T)
      if (!is.null(attr(p, 'status'))) {
        stop("error while generating plan")
      }
      task_dict <- yaml.load(p)

      task_map <- list()
      for (task_name in pl_tasks) {
        task_map[[task_name]] <- NA
      }

      mc_task_hash_list <- c()
      mc_task_name_map <- list()
      mc_task_counter = list()

      for (t_hash in task_dict$tg_list) {
        task <- task_dict$tg_dict[[t_hash]]
        if (task$name %in% names(task_map) & !is.null(task_map[[task$name]])) {
          if (is.na(task_map[[task$name]])) {
            task_map[[task$name]] <- t_hash
          }
        } else if (expand) {
          if (static_names) {
            mc_task_hash_list <- c(mc_task_hash_list, t_hash)
            mc_task_name_map[[task$name]] <- c(mc_task_name_map[[task$name]], t_hash)
            mc_task_counter[[task$name]] <- 0
          } else {
            task_map[[t_hash]] <- t_hash
          }
        }
      }

      if (length(mc_task_hash_list) > 0) {
        for (mc_task_name in names(mc_task_name_map)) {
          mc_task_list <- mc_task_name_map[[mc_task_name]]
          mc_task_list <- unique(mc_task_list)
          if (length(mc_task_list) > 1) {
            for (mc_task in mc_task_list) {
              counter <- mc_task_counter[[mc_task_name]]
              task_map[[paste0(mc_task_name, '_', counter+1)]] <- mc_task
              mc_task_counter[[mc_task_name]] <- counter + 1
            }
          } else {
            task_map[mc_task_name] <- mc_task_list[1]
          }
        }
      }

      plan <- Plan$new(task_map, task_dict)
      return (plan)
    }
  )
)

Plan <- R6Class(
  "Plan",
  lock_class = FALSE,
  lock_objects = FALSE,
  public = list(
    task_hash_map = NULL,
    hash_task_map = NULL,
    task_dict = NULL,
    initialize = function (task_map, task_dict) {
      self$task_hash_map = task_map
      self$hash_task_map = names(task_map)
      keys <- as.vector(unlist(unname(task_map)))
      names(self$hash_task_map) = keys
      self$task_dict = task_dict
      for (task_name in names(self$task_hash_map)) {
        if (is.null(self$task_hash_map[[task_name]])) {
          next
        }
        if (!is.na(self$task_hash_map[[task_name]])) {
          tdi <- TaskDebugger$new(task_dict$tg_dict[[self$task_hash_map[[task_name]]]])
          self$proto(task_name, tdi)
        }
      }
    },
    proto = function (task_name, tdi) {
      self[[task_name]] <- tdi
      environment(self[[task_name]]) <- environment(self$proto)
    },
    getExecOrder = function () {
      exec_order <- list()
      for (t_hash in self$task_dict$tg_list) {
        if (t_hash %in% names(self$hash_task_map)) {
          exec_order <- append(exec_order, self$hash_task_map[[t_hash]])
        } else {
          exec_order <- append(exec_order, t_hash)
        }
      }
      return (exec_order)
    },
    execTill = function(task_name) {
      exec_order <- self$getExecOrder()
      for (current_task in exec_order) {
        if (current_task == task_name) {
          break
        }
        if (current_task %in% names(self)) {
          tdi <- self[[current_task]]
        } else {
          tdi <- TaskDebugger$new(self$task_dict$tg_dict[[current_task]])
        }
        if (tolower(tdi$task$`_language`) != 'r') {
          print(paste0('Skipping ', tdi$task$name,  ' run because class language is ', tdi$task$`_language`))
        }
        else {
          tdi$loadInputs()
          tdi$run()
          tdi$saveOutputs()
        }
      }
    }
  )
)

TaskDebugger <- R6Class(
  "TaskDebugger",
  public = list(
    task = NULL,
    t_hash_dict = NULL,
    inputs = NULL,
    params = NULL,
    outputs = NULL,
    ti = NULL,
    log = NULL,
    context = NULL,
    initialize = function (task) {
      self$task = task
      self$t_hash_dict = task$hashes
      self$log = logger$getLogger(task$hashes$transform_hash, stderr=T, enable_cloud_logging=F)
      self$context = list(
        logger = self$log
      )
    },
    loadInputs = function () {
      inp <- executor$loadInputs(debug$store, self$t_hash_dict)
      self$inputs <- inp$inputs
      self$params <- inp$params
    },
    addTransformHashestoDB = function(t_hash_dict){
      for (output_name in names(t_hash_dict$hashes$outputs)) {
        out_hash <- t_hash_dict$hashes$outputs[[output_name]]
        hash_key <- paste0(out_hash, "_", debug$store$local_store$get_user())
        print(paste0("Adding to DatasetDB", hash_key))
        curr_time <- strftime(as.POSIXct(Sys.time()) , "%Y-%m-%dT%H:%M:%S%z")
        expire_time <- strftime(as.POSIXct(Sys.time()) + days(150), "%Y-%m-%dT%H:%M:%S%z")
        t_data <- list()
        t_data[['_id']] <- hash_key
        t_data[['path']] <- file.path(t_hash_dict$data_dir, out_hash)
        t_data[['user']] <- debug$store$local_store$get_user()
        t_data[['status']] <- DBStatus$PROCESSING
        t_data[['ti_type']] <- t_hash_dict$name
        t_data[['ttl']] <- t_hash_dict$ttl
        tryCatch(
          {
            upd_status <- debug$store$local_store$datasets$update(query = paste0('{"_id":"', hash_key, '"}'), 
                                      update = paste0('{"$set":', jsonlite::toJSON(t_data, auto_unbox=T), '}'),
                                      upsert = TRUE
                                      )
            upd_status <- debug$store$local_store$datasets$update(query = paste0('{"_id":"', hash_key, '"}'),
                        update = paste0('{"$set": {"timestamp" :  { "$date" :"', curr_time, 
                                        '" } , "expires_at": { "$date" :"', expire_time, '" } } }'),
                        upsert = TRUE)
          },
          error=function(cond) {
            print('Failed addingTransformHashtoDB')
            print(cond)
            return(FALSE)
          }
        )
      }
      return(TRUE)
    },
    apply = function () {
      if (tolower(self$task$`_language`) != 'r') {
        print(paste0('cannot run', self$task$`_language`, 'in R'))
        return (NULL)
      }
      self$ti <- executor$getInstance(self$t_hash_dict, self$inputs, self$params, context=self$context)
      if (!is.null(debug$tasks)){
        t_hash_dict <- debug$tasks$find(query = paste0('{"_id":"', self$t_hash_dict$transform_hash, '"}'), )
        self$addTransformHashestoDB(t_hash_dict)
      }
      self$ti$apply()
      self$outputs <- self$ti$outputs
    },
    run = function () {
      if (tolower(self$task$`_language`) != 'r') {
        print(paste0('cannot run', self$task$`_language`, 'in R'))
        return (NULL)
      }
      self$ti <- executor$getInstance(self$t_hash_dict, self$inputs, self$params, context=self$context)
      tryCatch(
        {
          self$ti$apply()
        },
        error=function(cond) {
          print('Did you run loadInputs() method before run?')
          print(paste0(self$task$name, " failed to apply, please check and run again"))
          print(cond)
          return()
        }
      )
      self$outputs = self$ti$outputs
    },
    loadOutputs = function () {
      self$outputs <- executor$loadOutputs(debug$store, self$t_hash_dict)
    },
    saveOutputs = function () {
      if (is.null(self$outputs)) {
        print('cannot save empty outputs')
        return (NULL)
      }
      executor$saveOutputs(debug$store, self$t_hash_dict, self)
    },
    saveOutputsToFile = function () {
      if (is.null(self$outputs)) {
        print('cannot save empty outputs')
        return (NULL)
      }
      executor$saveOutputsToFile(debug$store, self$t_hash_dict, self)
    },
    deleteInputs = function () {
      for (inp_name in names(self$t_hash_dict$inputs)) {
        debug$store$delete(self$t_hash_dict$inputs[[inp_name]])
      }
    },
    deleteOutputs = function () {
      for (out_name in names(self$t_hash_dict$outputs)) {
        debug$store$delete(self$t_hash_dict$outputs[[out_name]])
      }
    },
    getLog = function (timestamp=NULL) {
      base_url <- 'https://console.cloud.google.com/logs/query;query='
      query_params <- list()
      query_params <- paste0('logName=', 'projects/', Sys.getenv('GOOGLE_CLOUD_PROJECT'), '/logs/', self$t_hash_dict$transform_hash)
      query_params <- paste0(query_params, "&labels.deployment_type=", debug$store$local_store$db_env)
      
      user_related_dir_string <- unlist(strsplit(debug$remote_store_loc, 'users/'))[2]
      user_name <- unlist(strsplit(user_related_dir_string,'/'))[1]
      query_params <- paste0(query_params, "&labels.user=", user_name)

      url <- paste0(base_url, URLencode(query_params, reserved=TRUE), paste0('?project=', Sys.getenv('GOOGLE_CLOUD_PROJECT')))
      url <- stringr::str_replace_all(url, '%26', '%0A')
      print(paste0("Check log here ", url))
    },
    getLogListing = function () {
      self$getLog()
      # log_dir <- file.path(debug$store_loc, 'logs', self$task$hashes$transform_hash)
      # timestamps <- list.dirs(log_dir, full.names = F, recursive = F)
      # timestamps <- timestamps[order(timestamps, decreasing = T)]
      # logs <- list()
      # for (timestamp in timestamps) {
      #   logs[[timestamp]] <- as.POSIXct(as.numeric(timestamp), origin = "1970-01-01")
      # }
      # self$printList(logs)
    },
    printList = function (list_var) {
      list_vec <- NULL
      for (key in names(list_var)) {
        list_vec <- append(list_vec, paste0(key, ": ", list_var[[key]]))
      }
      writeLines(list_vec)
    }
  )
)

if (debug$IMPORT_FLAG == FALSE) {
  executor$importAll()
  debug$IMPORT_FLAG <- TRUE
}



