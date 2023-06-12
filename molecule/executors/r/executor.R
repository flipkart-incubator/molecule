library(argparse)
library(logging)
source("core/executors/r/storage.R")
source("core/executors/r/logger.R")
source("core/executors/r/gcpClient.R")

if (Sys.getenv('MONGO_URI') != '') {
  MONGO_URI <- Sys.getenv('MONGO_URI')
} else {
  MONGO_URI <- 'mongodb://mongo0:27017,mongo1:27017/'
}


importAll <- function (wd, log=logging::logwarn) {
  transform_files <- list.files(file.path(wd, "impl/rcode"), recursive = T, full.names = T, pattern = ".\\.R$")
  transform_files <- transform_files[!grepl(".*legacy.*", transform_files)]
  transform_files <- transform_files[!grepl("library_imports.R", transform_files)]

  tryCatch({
    source(file.path(wd, "impl/rcode/library_imports.R"))
   }, error = function (cond) {
      log(paste0('failed to import main library file, quitting transofrm'))
      print(cond)
  })

  for (file in transform_files) {
    tryCatch({
               suppressMessages(source(file))
               # source(file)
             },
             error = function(cond) {
               log(paste0('failed to import source file ', file))
               print(cond)
             }
    )
  }
}

initStore <- function (wd, store_loc, storage_type='nfs', db=NULL, remote_loc=NULL, db_env='stage') {
  store <- Storage$new(store_loc, defs_path=normalizePath(file.path(wd, '/infra/specs/datasets')), 
            mode=storage_type, db=db, remote_loc=remote_loc, db_env=db_env)
  return (store)
}

loadDict <- function (t_hash, cloud_client, db_env='stage') {
  transforms <- mongolite::mongo(collection = 'transforms', db = db_env, url = MONGO_URI)
  # t_hash_dict <- transforms$find(query = paste0('{"_id":"', t_hash, '"}'))
  iterator <- transforms$iterate(query = paste0('{"_id":"', t_hash, '"}'))
  t_hash_dict <- iterator$json()
  t_hash_dict <- yaml.load(t_hash_dict)

  return (t_hash_dict)
}

loadInputs <- function (store, t_hash_dict) {
  inputs <- list()

  inputs_type <- t_hash_dict$inputs_type
  params <- t_hash_dict$params

  for (input_name in names(t_hash_dict$inputs)) {
    if (typeof(t_hash_dict$inputs[[input_name]]) == "list") {
      inputs[[input_name]] <- list()
      for (in_ser in names(t_hash_dict$inputs[[input_name]])) {
        inputs[[input_name]][[in_ser]] <- store$load(t_hash_dict$inputs[[input_name]][[in_ser]], inputs_type[[input_name]])
      }
    }
    else {
      inputs[[input_name]] <- store$load(t_hash_dict$inputs[[input_name]], inputs_type[[input_name]])
    }
  }

  return (list(inputs=inputs, params=params))
}

loadOutputs <- function (store, t_hash_dict) {
  outputs <- list()

  outputs_type <- t_hash_dict$outputs_type

  for (output_name in names(t_hash_dict$outputs)) {
    if (typeof(t_hash_dict$outputs[[output_name]]) == "list") {
      outputs[[output_name]] <- list()
      for (in_ser in names(t_hash_dict$outputs[[output_name]])) {
        outputs[[output_name]][[in_ser]] <- store$load(t_hash_dict$outputs[[output_name]][[in_ser]], outputs_type[[output_name]])
      }
    }
    else {
      outputs[[output_name]] <- store$load(t_hash_dict$outputs[[output_name]], outputs_type[[output_name]])
    }
  }

  return (outputs)
}

getInstance <- function (t_hash_dict, inputs, params, context=list()) {
  t_class_name <- t_hash_dict$transform$transform_class
  t_cls <- get(t_class_name)

  ti <- t_cls$new(inputs, params, context = context)

  return(ti)
}

saveOutputs <- function (store, t_hash_dict, ti) {
  for (output_name in names(t_hash_dict$outputs)) {
    store$save(t_hash_dict$outputs[[output_name]], ti$outputs[[output_name]])
  }
}

saveOutputsToFile <- function (store, t_hash_dict, ti, ttl=28) {
  outputs_type <- t_hash_dict$outputs_type

  for (output_name in names(t_hash_dict$outputs)) {
    store$saveToFile(t_hash_dict$outputs[[output_name]], ti$outputs[[output_name]], outputs_type[[output_name]], ttl)
  }
}

deleteHashes <- function (store, t_hash_dict, ti) {
  if (!is.null(ti$outputs$delete_hashes)) {
    for (inp_name in ti$outputs$delete_hashes) {
      hash <- t_hash_dict$inputs[[inp_name]]
      store$delete(hash)
    }
    quit(save = "no", status = 1)
  }
}

main <- function () {
  arg_parser <- ArgumentParser(description='spawner for python-based transforms')
  arg_parser$add_argument('-t', '--t-hash', action='store', help='Transform Hash')
  arg_parser$add_argument('-s', '--store-loc', action='store', help='Store location for files')
  arg_parser$add_argument('-wd', '--working-dir', action='store', help='Working dir for files')
  arg_parser$add_argument('-ttl', '--time-to-live', action='store', help='TTL for files in days')
  arg_parser$add_argument('--storage-type', action='store', help='nfs or gcs storage')
  arg_parser$add_argument('--dev-env', action='store', help='stage or prod env')

  args <- arg_parser$parse_args()

  if (is.na(args[["time_to_live"]]) | (args[["time_to_live"]]=='None')) {
    args[["time_to_live"]] = NULL
  } else {
    args[["time_to_live"]] = as.numeric(args[["time_to_live"]])
  }

  cloud_client <- GCPClient$new()
  t_hash_dict <- loadDict(args[["t_hash"]], cloud_client, db_env=args[["dev_env"]])
  # Yaml loader of R is different, it treat single valued list as integer/string
  t_hash_dict <- yaml.load(as.yaml(t_hash_dict))
  user_related_dir_string <- unlist(strsplit(t_hash_dict[['data_dir']], 'users/'))[2]
  user_name <- unlist(strsplit(user_related_dir_string,'/'))[1]

  log <- logger$getLogger(args[["t_hash"]],  user=user_name, deployment_type=args[['dev_env']])
  log$info('Initializing Dict')

  store <- initStore(args[["working_dir"]], args[["store_loc"]], args[["storage_type"]], db=cloud_client, remote_loc=t_hash_dict[['data_dir']], db_env=args[["env"]])

  log$info('Importing Classes')
  importAll(args[["working_dir"]], log=log$warning)
  log$info('Loading Inputs')
  inp <- loadInputs(store, t_hash_dict)
  inputs <- inp$inputs
  params <- inp$params
  log$info('Running Transform')
  context <- list(
    logger = log
  )
  ti <- getInstance(t_hash_dict, inputs, params, context = context)
  ti$run()
  log$info("Checking for delete_hashes")
  deleteHashes(store, t_hash_dict, ti)
  log$info('Saving Outputs')
  saveOutputsToFile(store, t_hash_dict, ti, args[["time_to_live"]])

  quit(save = "no", status = 0)
}

if (!interactive()) {
  main()
}
