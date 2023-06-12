# Molecule

Molecule is a cross-language distributed platform to programmatically create, schedule, and automate forecasting workflows. Workflows are directed acyclic graphs(DAGs) of tasks associated with forecasting procedures such as data dumps, preprocessing, feature engineering and model train-infer.

**Maintainers**

- [Naidu KVM](https://github.com/naidukvm)
- [Suvigya Vijay](https://github.com/suvigyavijay)
- Piyush Vyas
- [Abhishek Laddha](https://github.com/abhishekladdha)
- [Mayank Kumar](https://github.com/mmanas15)

**Table of Contents**

1. [Getting Started](#getting-started)
2. [Running](#running)
3. [Usage](#usage)
4. [Advanced Usage](#advanced-usage)
5. [UI](#ui)
6. [Understanding Molecule](#understanding-the-molecule)
7. [Docker and Cluster Setup](#docker-and-cluster-setup)
8. [Feature Requests and Contributing](#feature-requests-and-contributing)
9. [Folder Structure](#folder-structure)
10. [License](#license)

## Getting Started

### Prerequisites

Please ensure you have the following setup in your local machine:

- Python 3.6+
- PyYaml 5.1+
- ZMQ
- Rsync 3.1+

Additional requirements for running tests(optional) on local machine:
- Pandas, feather, PyReadR, HDFSClient
- R 3.6+
- GCC
- MongoDB Server 4.x
- flask, pymongo, flask_cors, flask-pymongo, python-crontab
- yaml, logging, argparse, readr, feather

You can follow the following, in case you don't have any of them installed:

```
sudo apt-get update
sudo apt-get install ssh rsync mongodb-org
sudo apt-get python3 python3-pip
pip3 install pyyaml zmq hdfs flask pymongo flask_cors flask-pymongo python-crontab
```
**Note:** You might need to do installation via homebrew for Mac OS.

Also, you need these additional dependencies and R setup for running tests(optional):

```
pip3 install pandas feather-format pyreadr hdfs
sudo apt-get update
sudo apt-get install -yq --no-install-recommends r-base r-base-core r-recommended gcc
R -e "install.packages(c('filelock', 'logging', 'yaml', 'argparse', 'readr', 'feather'))"
```

### Running Tests (Optional)

_**Note:**_ Unit tests are deprecated, with current changes, please skip them.

Tests are important in case you want to contribute to the project. Its recommended you do the manual dummy pipeline runs if you just want to run locally.

You can run an e2e test on your local machine to ensure the setup is working, if not you can raise an issue on Github to talk to one of the authors regarding the same.

You can run tests via the following commands:
```
cd /path/to/molecule
python -m unittest
```

If all the tests are passing, you are good to go for debugging locally or making a PR.

## Running

Molecule supports four kinds of runs:

1. Local Platform - Local Spawner Runs
2. Cluster Platform - Local Spawner Runs
3. Cluster Platform - Cluster Spawner Staging Runs
3. Cluster Platform - Cluster Spawner Production Runs

This can be only done once you have created all four specified YAML files, written transform implementations(with metadata) and mapped server config and docker config. Dummy commands to test setup are provided, they are same as e2e tests.

_**Note:**_ All code should be run from `molecule` as working directory

### Local Platform - Local Spawner Runs

This requires all the basic prerequisites installed on your local machine. If you're not using docker for python and R, then you should have `python3` and `Rscript` in the environment variable `PATH` . If you prefer dockers, basic instructions are mentioned [here](#docker-and-cluster-setup).

1. Run Platform Locally, do not forget to configure `core/server_config.yaml` to point to your local settings appropriately. `root_dir` and `store_loc` should especially be taken care of.

```
python3 core/infrautils.py -c core/server_config.yaml -ss -sws

Arguments:
    -c          --config                Server Config YAML
    -ss         --start_services        Start Platform Server
    -sws        --start_web_services    Start Web Server
```

2. Bring up the spawners, single/multiple as you see fit. In case you want to use hive transforms, see [Cluster Platform - Local Spawner Runs](#cluster-platform---local-spawner-runs)

```
python3 core/spawner.py -cs <ServerIP>:<ServerPort> -p <Comma separated list of ports for spawners>  -s <StorageRoot> -wd <WorkDir or SourceDir of code> -rip <RemoteIP> -rsl <RemoteStorageRoot> -dc <DockerConfig> -mt <MachineType>

Arguments for spawner.py:
    -cs     --command_server        Platform/Command Server Address
    -p      --ports                 Ports for running the executor(comma-separated)
    -s      --store_loc             Store location for files
    -wd     --working_dir           Working directory for spawner
    -mt     --machine_type          Machine type, cpu, gpu, custom
    -rip    --remote_ip             Remote IP for online storage
    -rsl    --remote_store_loc      Remote storage location
    -dc     --docker_config         Docker Config File
    -o      --online                Connect to central online platform
    -d      --debug                 Debug mode to override working dir and store
```

**Example**
```
python3 core/spawner.py -cs localhost:5566 -p 8000,8001,8002,8003,8004,8005,8006,8007,8008,8009 -s /Users/<user>/store -wd ~/code/molecule/ -mt cpu
```

_Note:_ Provide `-dc` flag, docker config for attaching dockers.

3. Submit Pipeline

```
python3 core/infrautils.py -c <ServerConfig> -sp -pn <PipelineName as per spec> -ps <PipelineSpecFolder or ProjectFolder> -pc <PipelineConfig> -d <DebugRunMachine>

Arguments:
    -c      --config                    Server Config YAML
    -sp/kp  --submit/kill_pipeline      Submit/Kill Pipeline
    -pn     --pipeline_name             Pipeline Name
    -ps     --pipeline_spec_path        Pipeline Spec Path
    -pc     --pipeline_config_path      Pipeline Config
    -m      --commit_message            Commit Message
    -q      --queue                     Queue(p0, p1, p2). p2(default)
    -d      --debug                     Debug Run On Machine ("local", CustomName)
```

_Note:_ For local platform - local spawner runs `-d` arg should be provided as `local`

**Dummy Pipeline Runs**

- Basic Dummy
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_pipeline_config.yaml -d local
```

- Hive Dummy Run
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_hive_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_hive_pipeline_config.yaml -d local
```

- MapCombine Dummy Run
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_map_combine_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_map_combine_pipeline_config.yaml -d local
```

### Cluster Platform - Local Spawner Runs

This type of run uses the online platform server running online to schedule jobs on custom spawners(running on your laptop/local machine), whereas hive jobs are automatically mapped to hive spawner. This comes in handy when you have hive-dependent tasks in your pipeline and you want to run all other tasks on your local machine.

1. Start a custom spawner on your local machine

```
python3 core/spawner.py -cs <IP>:5566 -p <Comma separated list of ports for spawners>  -s <StorageRoot> -wd <WorkDir or SourceDir of code> -rip <IP> -rsl /mnt/data/<YourUsername>/store -dc <DockerConfig> -d -o -mt <CustomMachineType>

Arguments for spawner.py:
    -cs     --command_server        Platform/Command Server Address
    -p      --ports                 Ports for running the executor(comma-separated)
    -s      --store_loc             Store location for files
    -wd     --working_dir           Working directory for spawner
    -mt     --machine_type          Machine type, cpu, gpu, custom
    -rip    --remote_ip             Remote IP for online storage
    -rsl    --remote_store_loc      Remote storage location
    -dc     --docker_config         Docker Config File
    -o      --online                Connect to central online platform
    -d      --debug                 Debug mode to override working dir and store
```

**Example**
```
python3 core/spawner.py -cs <IP>:5566 -p 8000,8001,8002 -s /Users/<user>/store -wd /Users/<user>/code/moelcule/ -rip <IP> -rsl /mnt/data/<user>/store -d -o -mt <user>
```

2. Submit Pipeline

```
python3 core/infrautils.py -c <ServerConfig> -sp -pn <PipelineName as per spec> -ps <PipelineSpecFolder or ProjectFolder> -pc <PipelineConfig> -d <DebugRunMachine>

Arguments:
    -c      --config                    Server Config YAML
    -sp     --submit_pipeline           Submit Pipeline
    -pn     --pipeline_name             Pipeline Name
    -ps     --pipeline_spec_path        Pipeline Spec Path
    -pc     --pipeline_config_path      Pipeline Config
    -d      --debug                     Debug Run On Machine ("local", CustomName)
```

_Note:_ For local platform - local spawner runs `-d` arg should be same as `-mt` arg for custom spawner above. Don't use global machine_type as cpu, gpu or hive. It is recommended to put in your own username.

**Dummy Pipeline Runs**

- Basic Dummy
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_pipeline_config.yaml -d <user>
```

- Hive Dummy Run
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_hive_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_hive_pipeline_config.yaml -d <user>
```

- MapCombine Dummy Run
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_map_combine_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_map_combine_pipeline_config.yaml -d <user>
```

### Cluster Platform - Cluster Spawner Staging Runs

In cluster runs unlike local runs, platform and spawners are already set up online. You just need to submit the pipeline with planner.

**Note:** Please make a note this submission is done without `-d` arg which is marking this is not a local run.

```
python3 core/infrautils.py -c <ServerConfig> -sp -pn <PipelineName as per spec> -ps <PipelineSpecFolder or ProjectFolder> -pc <PipelineConfig>

Arguments:
    -c      --config                    Server Config YAML
    -sp     --submit_pipeline           Submit Pipeline
    -pn     --pipeline_name             Pipeline Name
    -ps     --pipeline_spec_path        Pipeline Spec Path
    -pc     --pipeline_config_path      Pipeline Config
```

**Example**
```
python3 core/infrautils.py -c core/server_config.yaml -sp -pn dummy_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_pipeline_config.yaml
```


### Cluster Platform - Cluster Spawner Production Runs

For prod runs, you should check-in the code, and mention `git_url` and `git_commit_id` should be provided in pipeline config YAML. Everything else is same as [Cluster Platform - Cluster Spawner Staging Runs](#cluster-platform---cluster-spawner-staging-runs)

### Killing Run

Pipelines/Runs can now be killed by replacing `-sp`(submit pipeline) flag to `-kp`(kill pipeline) flag.

```
python3 core/infrautils.py -c <ServerConfig> -kp -pn <PipelineName as per spec> -ps <PipelineSpecFolder or ProjectFolder> -pc <PipelineConfig>

Arguments:
    -c      --config                    Server Config YAML
    -kp     --kill_pipeline             Submit Pipeline
    -pn     --pipeline_name             Pipeline Name
    -ps     --pipeline_spec_path        Pipeline Spec Path
    -pc     --pipeline_config_path      Pipeline Config
```

**Example**
```
python3 core/infrautils.py -c core/server_config.yaml -kp -pn dummy_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_pipeline_config.yaml
```

## Usage

### Generating Metadata

You can generate metadata for your transforms after writing all YAMLs for your pipeline.
```
python3 core/metadata.py -m <TransformType>
```
**Example**
```
python3 core/metadata.py -m generate_random_number
```

### Logging

Logging enables you to save timestamped logs for your task, which you can easily access through debugging. Logs for each run is saved, so you can access all logs for a task, whenever you ran them. `stdout` and `print` statements are not saved by default, whereas any `stderr` will automatically be logged.

The API for across both R and Python is consistent, there is only change in the way to access, `.` in python and `$` in R. 

**Methods**

Inside the `apply()` method in transform's implementation, you can access logger methods via `self$logger$method_name(msg)` or  `self.logger.method_name(msg)`.

_Note:_ You can only pass one value(of uniform type such as str, int, list or dataframe) as a message, it can be any type. You can't do a call like `self.logger.method_name(msg1, msg2, ...)`. In case you want to print with multiple values, please use the command as `self.logger.method_name([msg1, msg2, msg3])`.

- debug(msg): Log debug level statements
- info(msg): Log general info messages
- warning(msg): Log warnings
- critical(msg): Log errors

#### Printing Pipeline Logs

You can get your pipeline logs by using this command.

```
python3 core/pl_logger.py -s <StoreLocation> -rsl <RemoteStoreLocations> -pn <PipelineName as per spec> -ps <PipelineSpecFolder or ProjectFolder> -pc <PipelineConfig> -d <MachineType>

Arguments:
    -s      --store_loc                 Store location for files
    -rsl    --remote_store_loc          Remote storage location
    -pn     --pipeline_name             Pipeline Name
    -ps     --pipeline_spec_path        Pipeline Spec Path
    -pc     --pipeline_config_path      Pipeline Config
    -d      --debug                     Machine Type, if Cluster PLatform - Local Spawner Run
```

_Example:_

```
python3 core/pl_logger.py -s /Users/<user>/store/ -pn dummy_hive_pipeline -ps projects/dummy -pc projects/dummy/configs/dummy_hive_pipeline_config.yaml -d <user> -rsl /mnt/data/<user>/store
```

### Debugging

The API for across both R and Python is consistent, there is only change in the way to access, `.` in python and `$` in R. Boilerplate to both the languages is provided at the end.

**Debug Methods**

These methods can be accessed by `debug.method_name` or `debug$method_name`.

- reimport(): Reimport all implementations of the language
- setOptions(store_loc, remote_ip, remote_store_loc, defs_path): setOptions will set global options for debugging environment
- generatePlan(pipeline_name, pipeline_spec_path, pipeline_config_path): generatePlan runs planner and generates the plan for given pipeline and returns it in a variable

**Plan Methods**

These methods are attached to generated plan from `debug.generatePlan(...)` call, and can be accessed by `plan.method_name` or `plan$method_name`.

- getExecOrder(): returns the execution order of pipeline
- execTill(task_name): runs pipeline upto the given task

Other variables in plan are dynamically attached by task_name in the spec file. For example, dummy pipelines plan will have variables like generate_random_number, make_dummy_list, etc. and can be accessed like `plan$generate_random_number`. These all are TaskDebugger type class methods.

**TaskDebugger Methods**

These methods are attached to task variable to help you see inputs, outputs, logs, and run the task. They can be accessed by `plan.task_name.method_name` or `plan$task_name$method_name`.

- loadInputs(): loads inputs to specified task in `plan.task_name.inputs`
- run(): runs the given task and adds outputs to `plan.task_name.outputs`, you should explicitly load inputs before
- loadOutputs(): load outputs of specified task in `plan.task_name.outputs`
- saveOutputs(): temporarily saves outputs in memory
- deleteInputs(): delete input data from local store
- deleteOutputs(): delete output data from local store
- saveOutputsToFile(): saves output to file in local/debug store location
- getLog(timestamp=None): returns the timestamped log for the task, or the latest log if not specified
- getLogListing(): prints list of logs available with timestamps

#### Python

In a Jupyter Notebook, or your debugging environment, paste this code to get started on debugging your pipeline.
```
molecule_path = '/path/to/molecule' # put the path to your molecule directory here
project_path = '/path/toproject_dir' # put the local path to your project directory here
store_loc = '/path/to/your/local/store' # put path to your local store location here

import os
import sys
sys.path.extend([molecule_path, os.path.join(molecule_path, 'core/'), os.path.join(project_path, 'impl/pycode')])
os.chdir(molecule_path)
from core.executors.py import debug

debug.setOptions(store_loc, project_path, mode='nfs')

p = debug.generatePlan('<PipelineName>', '<ProjectSpecPath>', '<ProjectConfigPath>') # replace with appropriate values
```

#### R

In Rmd or your debugging environment, paste this code to get started on debugging your pipeline.
```
molecule_path <- '/path/to/molecule' # put the path to your molecule directory here
project_path <- '/path/toproject_dir' # put the local path to your project directory here
store_loc <- '/path/to/your/local/store' # put path to your local store location here

setwd(molecule_path)
source("core/executors/r/debug.R")

debug$setOptions(store_loc, remote_ip, remote_store_loc, mode='nfs')

p = debug$generatePlan('<PipelineName>', '<ProjectSpecPath>', '<ProjectConfigPath>') # replace with appropriate values
```

### Data Validation

You can now validate data at any transform, making it fail. This also enables you to delete invalid input data generated at previous transforms. To enable deletion, provide a array/vector with input variable names attached to outputs by the key `delete_hashes`.

For reference you can look at DummyValidateData implementation in R and Python. They can be found under `impl/Xcode/transforms/dummy/dummy_validate_data.X`.

## Advanced Usage

TBD.

- Sub pipelines and use cases
- Generic Transforms
- Map Combine pipelines and use cases

## UI

UI is the frontend for Molecule tied up with MongoDB. With Molecule, you can visualize workflows, get their status and logs, and have reliable persistence of the runs you triggered. Moreover, any triggered workflow can be added to a project and monitored for long-term.

You can start molecule with `-sws` flag while starting the platform. Molecule UI is hosted on the port configured and its database is located at `db_path` and `db_name` mentioned in `server_config.yaml`.

## Understanding Molecule

### Basics

You should think of Molecule as a simple tool that helps you run your tasks in a defined sequence. We often interchange the term task with transforms and vice-versa, they are typically the same with negligible differences.

Any task/transform is defined by its inputs, params and outputs. It is also dependent on how the inputs were generated, which makes each task definition unique. But all you need to worry about is what are your inputs, params and outputs, as you shall see soon.

Molecule uses a programmatic approach to defining all your workflows via human-readable YAML files. These files are spread across the project, and are of four types:

1. Pipeline Spec YAML: This is the core specifications file that describes what the pipeline/workflow will be like. It defines tasks based on transform definitions.
2. Transform Def YAML: This is the transform definition file, where you define your transforms using the three things explained above inputs, params and outputs. Inputs and Outputs are defined by their own datasets(similar to a custom data structure) defined in dataset definitions.
3. Dataset Def YAML: This is the dataset definitions file where you define your data structure for the inputs and outputs you will be using throughout.
4. Pipeline Config File: This is the file which finally contains your exact parameters, class and related info, that is picked up by the other three files above, to generate a unique pipeline. This file is a single level file, don't nest YAML here.

Molecule is majorly based on these three components, the one you might hear often:

- **Planner:** Planner is the graph resolution framework that uses all the YAML files given above to resolve an execution flow for the Molecule to schedule and run tasks. It handles dependencies, execution order, versioning and metadata validations. Planner runs locally on your system and submits the generated plan to platform online.

Planner is the only component an end-user should be concerned with, all other components are irrelevant as they are part of the core code.

- **Platform:** Platform accepts a pipeline submission and schedules your jobs according to available workers.
- **Spawner:**  Spawner sits on every worker accepting jobs from the platform and processing them appropriately.

### Deep Dive

TBD.

- Design Principles
- Core Components
- Extensibility

## Docker and Cluster Setup

### Docker Setup

We have made docker images containing all packages that may be required for our projects. You can set up docker on your local machine to get started quickly.

Run the following commands after the local docker setup:

**R Docker**
```
docker pull <DOCKER_IMAGE>:<TAG>
docker run -it -d -v /mnt/data:/mnt/data -p 1111:1111 -p 1187:8787 --name baseR <DOCKER_IMAGE>:<TAG>
```
**Python Docker**
```
docker pull <DOCKER_IMAGE>:<TAG>
docker run -it -d -v /mnt/data:/mnt/data -p 1313:1313 --name dlcPy <DOCKER_IMAGE>:<TAG>
```
After you have dockers running on your machine, modify `core/docker_config_map.yaml` appropriately.

## Feature Requests and Contributing

Feature requests are welcome. Please open a GitHub issue for a feature request. We may discuss the importance and utility that feature provides and will be added according to priority and bandwidth.

Contributing to the core code can only be done upon discussion with maintainers. Contributions shall not be picked up manually by anyone apart from maintainers. Any PR changing core code will hence be denied. For more details check CONTRIBUTING.md

New projects should be added only through PR when a complete local run is done. Care should be taken that one adds appropriate metadata and should not collide with already existing projects.

