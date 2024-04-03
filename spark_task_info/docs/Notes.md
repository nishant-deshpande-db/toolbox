## Some notes about Spark Task Info (STI)

STI was developed to identify outlier tasks. Outlier => tasks that take a lot longer (wall clock time) than others in the same stage.

There was an hypothesis that some tasks were "hanging" and causing jobs to sit waiting for the one hanging task to complete for a long time.

(Spark has speculative execution as a mitigation for this, but nevertheless the thought was to identify such "hanging task" jobs.)

There are known reasons for outlier tasks:
 - Data skew.
 - Processing skew caused by outlier data. For example, some pathological json that hangs the json parser.

Other reasons are suspected:
 - Bad VM which "hangs".
 - Some cloud (or other) API call that hangs.
 - Some UDF that uses some library with bugs that hangs.

The first task (no pun intended) was to get data and identify outliers.

By joining task metrics (one row per task) to aggregated stage metrics, tasks that are X time the stage p95 can be flagged. (They are flagged in the gt_p95_10_* columns in the result data).

The next task was to identify tasks that were outliers *without explanation*. I.e. without known reasons like data skew. If a task is an outlier (duration > 10 x p95 duration of tasks in the stage), but none of the other metrics are similar outliers, then the task could be considered an unexplained outlier.

Some of the flags in the result data identify such tasks.

The result of this analysis on some specific set of jobs was that less than 0.1% of tasks were unexplained outliers, and so this was not the main cause of long runtimes, but rather explained outliers were the main problem.

### More notes and problems not resolved (yet)

There are some unresolved problems with "unexplained outliers". For example, one of the metrics is "executor_cpu_time". An outlier task due to `time.sleep` would clock up executor_cpu_time, and so this metric should always (?) be proportional to the task duration (??). A test with `time.sleep` showed this as expected, as did a test where the task called an http api that returned after 180s, making this single task an outlier. The task metric cpu_time was still proportional.

But some customer analyzed jobs turned up "unexplained outlier tasks". I.e. the task duration was not explained even with this. Unfortunately those tasks were not analyzed further.

### Querying the results table

The class SparkTasksWithStageEDA has some queries that show such tasks. (EDA = Exploratory Data Analysis.) They are not documented yet.

#### Columns in the results table

task_metrics = struct with all the task metrics i.e. those in the task metrics section of the task end event logs.

task_accumulables = struct with all the task accumulables i.e. in the accumulables section of the task end event logs.

task_* = general task info like start, end times.

stage_[avg|median|p95]_* = stage aggregates of each metric over all tasks in the stage.

stage__* = basic stage information (start time, end time..)

gt_p95_[5|10]_* = flag indicating if a task was greater than the p95*[5|10] of the metric for the stage.

overall_score = sum of the gt_* flags.

#### Misc

Unclear if all the metrics should have been columns, vs in the structs. Or if the stage_* and gt_* columns should have been in structs. Structs are harder to grep, but lots of columns make things generally uglier.





