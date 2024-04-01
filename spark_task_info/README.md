## Spark Task Info (STI)

### Summary

STI provides a table/dataframe with one row per task.
Each row contains
- All task metrics found in the eventlog.
- Stage metrics (denormalized so they are in every row).
- Aggregated task metrics at the stage level (avg, min, max, p95).
- Flags (see below)

STI parses the eventlogs you point it at, constructs above, and saves it to a table.
The eventlogs can be on cloud store, or in (regional) logfood.

### Installing

```pip install spark_task_info-<version>-py3-none-any.whl```

### Using

```
import spark_task_info
```

##### From cloud store logs
```
eventlogs = [
  'dbfs:/cluster-logs/0214-184635-fdf7gq60/eventlog/0214-184635-fdf7gq60_10_251_129_89/*/',
  'dbfs:/cluster-logs/0214-184635-fdf7gq60/eventlog/0214-184635-fdf7gq60_10_251_129_145/*/']
cluster_id = '0214-184635-fdf7gq60'

result_tbl = spark_task_info.SparkTasksWithStageInfo.createDataFromLogs(
  eventlogs=eventlogs, cluster_id=cluster_id)

```
##### From logfood
```
df = spark.sql("""
  select *
  from hive_metastore.prod_ds.spark_logs
  where true
  and workspaceId = '6968503516009187'
  and date between ('2024-02-20', '2024-02-21')
  and logType = 'eventlog'
  and logMessage:Event in ('SparkListenerTaskEnd', 'SparkListenerStageCompleted')
  and clusterId = '1220-000404-yh4um30h'
""")

result_tbl = spark_task_info.createDataFromEventsTable(df)
```


#### Some simple queries on the result table.

##### Get extreme outlier tasks (hopefully none!)
```
-- tasks that took longer than the p95 task duration for a stage,
-- and if that is explained by the task metrics.
select task_id, task_time as task_duration,
  if(overall_score > 0, 1, 0) as is_explained, 
  stage_avg_task_time, stage_median_task_time, stage_p95_task_time
from {result_table}
where stage_id = 4
and gt_p95_10 = 1
order by task_duration
```

##### Get tasks for a stage ordered by task_duration with selected metrics.
```
select task_id, task_time as task_duration,
  stage_avg_task_time, stage_median_task_time, stage_p95_task_time,
  task_launch_time, task_finish_time, task_end_reason, 
  task_metrics.disk_bytes_spilled, task_metrics.result_size, task_metrics.input_metrics__bytes_read,
  task_accumulables.cloud_storage_request_count, task_accumulables.cloud_storage_retry_count,
  task_metrics, task_accumulables
from {result_table}
where stage_id = 4
order by task_duration desc;
```

##### Stages by duration ordered by stage start time.
```
with t as (
  select stage_id,
  from_unixtime(cast(stage__submission_time as bigint)/1000) sst,
  from_unixtime(cast(stage__completion_time as bigint)/1000) sct, 
  *  
  from {result_table}
)
select distinct stage_id, sst, sct, timediff(second, sst, sct) duration,
  stage_num_tasks, stage_avg_task_time
from t
order by 2;
```

### Note

The results table has a lot of columns, and they are somewhat cryptic until you get used to them.

Read the detailed documentation to understand them. They do have some method to them.