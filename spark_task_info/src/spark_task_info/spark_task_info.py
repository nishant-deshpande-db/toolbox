# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import json, datetime, random, re
from typing import List, Any, Dict, Set, Pattern, Union, Optional
from pyspark.sql import Row, DataFrame

from typing import Callable
from functools import partial

import copy


# COMMAND ----------

__version__ = '0.1.6'
print(__version__)

# COMMAND ----------

from pyspark.sql import SparkSession

def get_or_create_spark_session() -> SparkSession:
    """
    Gets the current active Spark session or creates one if it does not exist.
    
    Returns:
        SparkSession: The Spark session.
    """
    #print("get_or_create_spark_session() called")
    spark_session = SparkSession.getActiveSession()
    
    if spark_session is None:
        spark_session = SparkSession.builder \
            .appName("My Spark Application") \
            .getOrCreate()
    
    return spark_session

# No. This module gets imported in workers. Can't create a spark session in a worker.
# spark = get_or_create_spark_session()


# COMMAND ----------



# COMMAND ----------

class Keys:
  _task_metrics_keys = [
    'executor_deserialize_time',
    'executor_deserialize_cpu_time',
    #'executor_run_time',   # On one test dataset, the correlation to task_time is 0.999. This might effectively be the task duration.
    'executor_cpu_time',
    'peak_execution_memory',
    'result_size',
    'jvm_gc_time',
    'result_serialization_time',
    'memory_bytes_spilled',
    'disk_bytes_spilled',
    'shuffle_read_metrics__remote_blocks_fetched',
    'shuffle_read_metrics__local_blocks_fetched',
    'shuffle_read_metrics__fetch_wait_time',
    'shuffle_read_metrics__remote_bytes_read',
    'shuffle_read_metrics__remote_bytes_read_to_disk',
    'shuffle_read_metrics__local_bytes_read',
    'shuffle_read_metrics__total_records_read',
    'shuffle_read_metrics__remote_requests_duration',
    'shuffle_read_metrics__push_based_shuffle__corrupt_merged_block_chunks',
    'shuffle_read_metrics__push_based_shuffle__merged_fetch_fallback_count',
    'shuffle_read_metrics__push_based_shuffle__merged_remote_blocks_fetched',
    'shuffle_read_metrics__push_based_shuffle__merged_local_blocks_fetched',
    'shuffle_read_metrics__push_based_shuffle__merged_remote_chunks_fetched',
    'shuffle_read_metrics__push_based_shuffle__merged_local_chunks_fetched',
    'shuffle_read_metrics__push_based_shuffle__merged_remote_bytes_read',
    'shuffle_read_metrics__push_based_shuffle__merged_local_bytes_read',
    'shuffle_read_metrics__push_based_shuffle__merged_remote_requests_duration',
    'shuffle_write_metrics__shuffle_bytes_written',
    'shuffle_write_metrics__shuffle_write_time',
    'shuffle_write_metrics__shuffle_records_written',
    'input_metrics__bytes_read',
    'input_metrics__records_read',
    'output_metrics__bytes_written',
    'output_metrics__records_written'
    ]

  _accumulables_keys = [
    'auto_batch_size_peak_batch_memory',
    'avg_hash_probe_steps_per_batch',
    'avg_hash_probes_per_row',
    'cache_hits_size',
    'cache_hits_size_uncompressed_',
    'cache_misses_size',
    'cache_misses_size_uncompressed_',
    'cache_rescheduled_misses_size',
    'cache_rescheduled_misses_size_uncompressed_',
    'cache_true_misses_size',
    'cache_true_misses_size_uncompressed_',
    'cache_writes_size',
    'cache_writes_size_uncompressed_',
    'cache_writes_stored_compressed_size',
    'cache_writes_stored_uncompressed_size',
    'cloud_storage_request_count',
    'cloud_storage_request_duration',
    'cloud_storage_request_size',
    'cloud_storage_requests_to_fetch_parquet_footer',
    'cloud_storage_response_size',
    'cloud_storage_retry_count',
    'cloud_storage_retry_duration',
    'columns_with_filters',
    'columns_with_filters_dictionary_encoded',
    'cumulative_time',
    'data_filters_rows_skipped',
    'dictionary_filtering_num_row_groups_eligible',
    'dictionary_filtering_num_row_groups_evaluated',
    'exclusive_time',
    'executor_time_cpu',
    'executor_time_cpu_applying_data_filters',
    'executor_time_cpu_applying_dictionary_filters',
    'executor_time_cpu_auto_batch_size_reset',
    'executor_time_cpu_cache_operations',
    'executor_time_cpu_decoding_columns',
    'executor_time_cpu_decoding_deletion_vectors',
    'executor_time_cpu_decoding_footers',
    'executor_time_cpu_decompressing_pages',
    'executor_time_cpu_distinct_evaluation_total',
    'executor_time_cpu_other',
    'executor_time_io_wait',
    'executor_time_io_wait_aether_metadata',
    'executor_time_io_wait_deletion_vectors',
    'executor_time_io_wait_footers',
    'executor_time_io_wait_full_files_column_data',
    'internal_metrics_executorcputime',
    'internal_metrics_executordeserializecputime',
    'internal_metrics_executordeserializetime',
    'internal_metrics_executorruntime',
    'internal_metrics_input_bytesread',
    'internal_metrics_input_recordsread',
    'internal_metrics_io_requestbytescount',
    'internal_metrics_io_requestcount',
    'internal_metrics_io_requestmsduration',
    'internal_metrics_io_responsebytescount',
    'internal_metrics_io_retrycount',
    'internal_metrics_io_retrydelaymsduration',
    'internal_metrics_jvmgctime',
    'internal_metrics_photonbufferpoolmemorysize',
    'internal_metrics_resultsize',
    'internal_metrics_shuffle_write_byteswritten',
    'internal_metrics_shuffle_write_recordswritten',
    'internal_metrics_trackedoffheapmemorysize',
    'maximum_size_of_1st_varlen_key',
    'minimum_size_of_1st_varlen_key',
    'num_batches_aggregated_in_sparse_mode',
    'num_bytes_spilled_to_disk_due_to_memory_pressure',
    'num_bytes_used_for_var_len_data',
    'num_bytes_written',
    'num_early_close',
    'num_hash_lookups',
    'num_spill_partitions_created',
    'num_uncompressed_bytes_spilled',
    'num_uncompressed_bytes_written',
    'number_of_non_local_rescheduled_scan_tasks',
    'number_of_output_batches',
    'number_of_output_rows',
    'number_of_partitions',
    'peak_memory_usage',
    'peak_reader_threads_utilized',
    'peak_untracked_memory_usage',
    'repeated_reads_estimated_high_size',
    'repeated_reads_estimated_low_size',
    'row_groups_in_files_to_read_total',
    'row_groups_read_actual',
    'rows_scanned',
    'runtime_filter_compressed_output_size_in_bytes',
    'runtime_filter_uncompressed_output_size_in_bytes',
    'sampling_and_range_boundary_calculation_time_in_ns',
    'shuffle_checksum_expression_evaluation_time_in_ns',
    'shuffle_partition_expression_evaluation_time',
    'shuffle_partitioning_type',
    'size_of_data_read_with_io_requests',
    'size_of_row_groups_before_filtering',
    'stage_duration',
    'tasks_total',
    'time_for_merging_spill_files_in_shuffle',
    'time_in_hash_build',
    'time_in_produce',
    'time_spent_compacting_batches',
    'time_spent_in_resizing_hash_table',
    'time_spent_reading_deletion_vectors',
    'time_taken_to_accumulate_rows',
    'time_taken_to_build_runtime_filter',
    'time_taken_to_compress_data',
    'time_taken_to_convert_columns_to_rows',
    'time_taken_to_convert_columns_to_rows_part_of_shuffle_file_write_',
    'time_taken_to_output_runtime_filter',
    'time_taken_to_reserve_memory',
    'time_taken_to_sort_rows_by_partition_id_part_of_shuffle_file_write_',
    'time_taken_waiting_on_file_write_io_part_of_shuffle_file_write_',
    'total_time_spent_producing_shuffle_files',
    'unified_cache_populate_time_for_parquetcolumnchunk',
    'unified_cache_populate_time_for_parquetfooter',
    'unified_cache_populate_time_for_range',
    'unified_cache_read_bytes_for_parquetcolumnchunk',
    'unified_cache_read_bytes_for_parquetfooter',
    'unified_cache_read_bytes_for_range',
    'unified_cache_serve_bytes_for_parquetcolumnchunk',
    'unified_cache_serve_bytes_for_parquetfooter',
    'unified_cache_serve_bytes_for_range',
    'unified_cache_write_bytes_for_parquetcolumnchunk',
    'unified_cache_write_bytes_for_parquetfooter',
    'unified_cache_write_bytes_for_range']
  


# COMMAND ----------

# Globals + UDF def

_pattern: Pattern[str] = re.compile(r'[^a-zA-Z0-9_]+')

def _canonicalize(s:str) -> str:
  return _pattern.sub('_', s).lower()

def _create_metrics_dict_from_d(d:dict, prefix:str=None) -> dict:
  kd = {}
  _prefix = ""
  if prefix:
    _prefix = f"{prefix}__"
  try:
    for (k,v) in d.items():
        #print(k,v)
        _k = _canonicalize(k)
        if type(v) == int:
            kd[f"{_prefix}{_k}"] = v
        elif type(v) == dict:
          _kd = _create_metrics_dict_from_d(v, prefix=f"{_prefix}{_k}")
          kd.update(_kd)
  except Exception as e:
    print(f"EXCEPTION: {e}")
    raise      
  return kd

def _create_metrics_dict(json_str:str) -> dict:
  js = json.loads(json_str)
  return _create_metrics_dict_from_d(js['Task Metrics'])

def create_task_metrics_json(json_str: str) -> str:
  try:
    result = _create_metrics_dict(json_str)
    #result = cls._create_metrics_dict(json_str)
    return json.dumps(result)
  except Exception as e:
    # TODO: something better??
    #print(f"Exception: {e}")
    #print(json_str)
    #raise
    return json.dumps({})

create_task_metrics_udf_json = udf(create_task_metrics_json, StringType())


# COMMAND ----------


def _extract_keys_from_accumulables(
    accumulables_set:set, 
    json_str: str) -> dict:
    task = json.loads(json_str)
    accumulables:list = task.get('Task Info', {}).get("Accumulables", [])
    # Important to sort so that the latest for each key ("Name") is at the end.
    acc = sorted(accumulables, key=lambda x: x['ID'], reverse=False)
    result = {}
    try:
        for item in acc:
            try:
                name = _canonicalize(item.get("Name"))  # Convert name to a valid column name format
                if name not in accumulables_set:
                   continue
                result[f"{name}_id"] = item.get("ID")
                result[f"{name}"] = int(item.get("Value"))
            except Exception as e:
                #raise ExtractKeysUDFException("Error processing item", json_str, e)
                #global_accumulables_errors.add(1)
                pass
        #return result
    except Exception as e:
        print(f"EXCEPTION: {e}")
        raise  # I want to know why this might fail and ignore specific fails if required
    # for k in accumulables_struct.fieldNames():
    #     if k not in result:
    #         result[k] = None
    return result

def create_extract_keys_from_accumulables(accumulables_set:set) -> Callable[[str], dict]:
    return partial(
        lambda x: _extract_keys_from_accumulables(accumulables_set=accumulables_set, json_str=x))

fn: Callable[[str], dict] = create_extract_keys_from_accumulables(set(Keys._accumulables_keys))
def extract_keys_from_accumulables_json(json_str: str) -> str:
    result = fn(json_str)
    return json.dumps(result)
extract_keys_udf_json = udf(extract_keys_from_accumulables_json, StringType())


# COMMAND ----------

class SparkTasksWithStageInfo:
  @classmethod
  def _spark_session(cls) -> SparkSession:
    return get_or_create_spark_session()

  @classmethod
  def _create_task_metrics_sql(cls, spark_event_log_table:str, sql_filter:str='') -> str:
    # The commented out metrics below are extracted along 
    if sql_filter:
      sql_filter = f"and {sql_filter}"
    sql_str = f"""
      with t as (
        select 
          logMessage:['Stage ID'] as stage_id,
          -- eventTime as event_time,   -- This only works on logs in logfood
          logMessage:['Task Info']['Launch Time'] as event_time,
          logMessage:['Task End Reason']['Reason'] as task_end_reason,
          logMessage:['Task Info']['Finish Time'] as task_finish_time,
          logMessage:['Task Info']['Launch Time'] as task_launch_time,
          logMessage:['Task Info']['Finish Time'] - logMessage:['Task Info']['Launch Time'] as task_time,
          logMessage:['Task Info']['Task ID'] as task_id,
          logMessage:['Task Info']['Executor ID'] as executor_id,
          logMessage:['Task Info']['Host'] as host,
          logMessage:['Task Info']['Locality'] as locality,
          logMessage:['Task Info']['Speculative'] as speculative,
          logMessage:['Task Type'] as task_type,
          logMessage
        from {spark_event_log_table}
        where true
        and logMessage:Event in ('SparkListenerTaskEnd')
      )
      select * from t
      where true
      {sql_filter}
    """
    print(f"[_create_task_metrics_sql] returning sql:\n{sql_str}")
    return sql_str

  @classmethod
  def _create_metrics_struct(cls, l:list):
    metrics_struct = StructType()
    for e in l:
      _sf = StructField(e, LongType(), True)
      metrics_struct.add(_sf)
    return metrics_struct

  @classmethod
  def _create_accumulables_struct(cls, accumulable_keys:list=None) -> StructType:
    if not accumulable_keys:
      accumulable_keys = Keys._accumulables_keys
    ak = [_canonicalize(s) for s in accumulable_keys]
    accumulables_struct = StructType()
    for k in ak:
      _sf = StructField(k, LongType(), True)
      accumulables_struct.add(_sf)
      _sf = StructField(k + '_id', IntegerType(), True)
      accumulables_struct.add(_sf)
    #print(accumulables_struct)
    return accumulables_struct

  @classmethod
  def _create_stages_sql(cls, spark_event_log_table:str, sql_filter:str='') -> str:
    if sql_filter:
      sql_filter = f"and {sql_filter}"
    sql_str = f"""
      with t as (
        select 
          logMessage:['Stage Info']['Stage ID'] as stage__id, 
          logMessage:['Stage Info']['Stage Attempt ID'] as stage__attempt_id, 
          clusterId as stage__cluster_id,
          -- eventTime as stage__event_time, 
          logMessage:['Stage Info']['Submission Time'] as stage__event_time,
          logMessage:['Stage Info']['Submission Time'] as event_time,   -- give all events an event_time for easy filtering
          logMessage:Event as event, 
          logMessage:['Stage Info']['Number of Tasks'] stage__num_tasks, 
          logMessage:['Stage Info']['Submission Time'] as stage__submission_time, 
          logMessage:['Stage Info']['Completion Time'] as stage__completion_time,
          logMessage as stage__logMessage
        from {spark_event_log_table}
        where true
        and logMessage:Event = 'SparkListenerStageCompleted'
      )
      select * from t
      where true
      {sql_filter}
    """
    return sql_str

  @classmethod
  def _add_stage_info_for_tasks(cls, task_table:str, stage_tbl:str) -> str:
    # stage_id's can be reused for the same cluster_id.
    # The cluster_id is reused across cluster restarts.
    # So need to use the timestamps to group the tasks to the stage correctly.
    # I've seen a task in a stage have a finish time a few seconds after the stage finish timestamp!
    # So give it 30 seconds beyond.
    sql = f"""
      select * except (st.event_time)
      from {task_table} t
      join {stage_tbl} st on (
        t.stage_id = st.stage__id
        and t.task_launch_time >= st.stage__submission_time
        and t.task_finish_time <= (st.stage__completion_time + 30000)
      )
    """
    return sql

  @classmethod
  def _create_sql_metrics_for_tasks(
    cls, 
    accumulables_keys:list,                                
    task_metrics_keys:list,
    base_table:str) -> str:
    pct_multiple_1 = 5
    pct_multiple_2 = 10
    pct_m = 95

    sql_accumulables_metrics = []
    sql_accumulables_comparison = []
    sql_accumulables_overall_score = []
    for e in accumulables_keys:
      k = _canonicalize(e)
      k2 = f'task_accumulables.{k}' 
      sql_accumulables_metrics.append(f"avg({k2}) as stage_avg_{k}, median({k2}) as stage_median_{k}, percentile({k2}, {pct_m/100}) as stage_p{pct_m}_{k}")
      sql_accumulables_comparison.append(f"if (t1.{k2} > stage_p{pct_m}_{k} * {pct_multiple_1}, 1, 0) as gt_p{pct_m}_{pct_multiple_1}_{k}")
      sql_accumulables_comparison.append(f"if (t1.{k2} > stage_p{pct_m}_{k} * {pct_multiple_2}, 1, 0) as gt_p{pct_m}_{pct_multiple_2}_{k}")
      sql_accumulables_overall_score.append(f"gt_p{pct_m}_{pct_multiple_1}_{k}")
      sql_accumulables_overall_score.append(f"gt_p{pct_m}_{pct_multiple_2}_{k}")

    sql_accumulables_metrics_str = ',\n'.join(sql_accumulables_metrics)
    sql_accumulables_comparison_str = ',\n'.join(sql_accumulables_comparison)
    sql_accumulables_overall_score_str = "(" + " + ".join(sql_accumulables_overall_score) + ") as accumulables_overall_score"


    sql_task_metrics = []
    sql_task_comparison = []
    sql_task_overall_score = []
    for e in task_metrics_keys:
      k = _canonicalize(e)
      k2 = f'task_metrics.{k}' 
      sql_task_metrics.append(f"avg({k2}) as stage_avg_{k}, median({k2}) as stage_median_{k}, percentile({k2}, {pct_m/100}) as stage_p{pct_m}_{k}")
      sql_task_comparison.append(f"if (t1.{k2} > stage_p{pct_m}_{k} * {pct_multiple_1}, 1, 0) as gt_p{pct_m}_{pct_multiple_1}_{k}")
      sql_task_comparison.append(f"if (t1.{k2} > stage_p{pct_m}_{k} * {pct_multiple_2}, 1, 0) as gt_p{pct_m}_{pct_multiple_2}_{k}")
      sql_task_overall_score.append(f"gt_p{pct_m}_{pct_multiple_1}_{k}")
      sql_task_overall_score.append(f"gt_p{pct_m}_{pct_multiple_2}_{k}")

    sql_task_metrics_str = ',\n'.join(sql_task_metrics)
    sql_task_comparison_str = ',\n'.join(sql_task_comparison)
    sql_task_overall_score_str = "(" + " + ".join(sql_task_overall_score) + ") as task_metrics_overall_score"

    sql_overall_score_str = "accumulables_overall_score + task_metrics_overall_score as overall_score"
    #sql_overall_score_str = "task_metrics_overall_score as overall_score"

    # print(sql_accumulables_metrics_str)
    # print("\n\n")
    # print(sql_accumulables_comparison_str)
    # print("\n\n")
    # print(sql_accumulables_overall_score_str)

    # Unclear if should use stage__attempt_id to group by.
    # Make sure the t1 join t2 is the same as the join in def add_stage_info_for_tasks
    sql = f"""
      with t2 as (
        select stage__cluster_id, stage_id, 
          -- stage__attempt_id,
          stage__submission_time,
          stage__completion_time,
          count(1) as stage_num_task_finish_events, 
          count(distinct(task_id)) as stage_num_tasks, 
          avg(task_time) as stage_avg_task_time, 
          median(task_time) as stage_median_task_time, 
          percentile(task_time, 0.95) as stage_p95_task_time,
          {sql_accumulables_metrics_str},
          {sql_task_metrics_str}
        from {base_table}
        group by 
          stage__cluster_id, 
          stage_id, 
          -- stage__attempt_id,
          stage__submission_time,
          stage__completion_time
      ),
      t3 as (
        select 
          if (t1.task_time > stage_p95_task_time * 5, 1, 0) as gt_p95_5,
          if (t1.task_time > stage_p95_task_time * 10, 1, 0) as gt_p95_10,
          if (t1.task_time > stage_median_task_time * 10, 1, 0) as gt_median_10,
          {sql_accumulables_comparison_str},
          {sql_accumulables_overall_score_str},
          {sql_task_comparison_str},
          {sql_task_overall_score_str},
          {sql_overall_score_str},
          t2.*, t1.* except (stage__cluster_id, stage_id, stage__submission_time, stage__completion_time)
        from {base_table} as t1
        join t2 on (
          t1.stage__cluster_id = t2.stage__cluster_id
          and t1.stage_id = t2.stage_id
          and t1.task_launch_time >= t2.stage__submission_time
          and t1.task_finish_time <= t2.stage__completion_time + 30000  -- look for 30000 above
        )
      )
      select 
        *
      from t3
      """
    return sql

  @classmethod
  def _create_sql_metrics(cls,
    accumulables_keys:list,
    task_metrics_keys:list,
    task_and_stage_data:DataFrame, # metrics_extract_table:str, 
    #final_result_table:str,
    #overwrite:bool=False
    ) -> DataFrame:
    task_and_stage_data_v = 'task_and_stage_data_v'
    task_and_stage_data.createOrReplaceTempView(task_and_stage_data_v)
    metrics_sql = cls._create_sql_metrics_for_tasks(
      accumulables_keys=accumulables_keys,
      task_metrics_keys=task_metrics_keys,
      base_table=task_and_stage_data_v)
    return cls._spark_session().sql(metrics_sql)

  @classmethod
  def _create_task_extract_metrics_df(cls,
    base_df:DataFrame, 
    task_metrics_keys:list=None) -> DataFrame:
    if not task_metrics_keys:
      task_metrics_keys = Keys._task_metrics_keys

    task_metrics_struct = cls._create_metrics_struct(task_metrics_keys)

    df_with_extracted_info:DataFrame = base_df
    df_with_extracted_info = (df_with_extracted_info
      .withColumn("task_metrics_json", create_task_metrics_udf_json(col("logMessage")))
      .withColumn("task_metrics", from_json("task_metrics_json", task_metrics_struct))
      #.drop("extracted_info_json")
    )

    return df_with_extracted_info


  @classmethod
  def _create_task_extract_accumulables_df(cls,
    base_df:DataFrame, 
    filter_1:str=None,
    accumulables_keys:list=None) -> DataFrame:
    if not accumulables_keys:
      accumulables_keys = Keys._accumulables_keys

    accumulables_struct = cls._create_accumulables_struct(accumulables_keys)

    df_with_extracted_info:DataFrame = base_df
    if filter_1:
      df_with_extracted_info = df_with_extracted_info.filter(filter_1)

    df_with_extracted_info = (df_with_extracted_info
      .withColumn("task_accumulables_json", extract_keys_udf_json(col("logMessage")))
      .withColumn("task_accumulables", from_json("task_accumulables_json", accumulables_struct))
      #.drop("task_accumulables_json")
    )

    return df_with_extracted_info

  @classmethod
  def _materialize(cls, df:DataFrame, table_name:str, analyze:bool=False):
    '''Materialize df into table_name.
    Overwrite table_name if present. No checks.
    '''
    df.writeTo(table_name).createOrReplace()
    if analyze:
      cls._spark_session().sql(f"analyze table {table_name} compute statistics")

  # -----------
  # public APIs

  @classmethod
  def create_task_data(
    cls, 
    event_logs_table:str,  # create a view from a df if required
    accumulables_keys:list,
    task_metrics_keys:list,
    sql_filter:str='') -> DataFrame:
    '''Parse event_logs and return a DataFrame of task data.
    event_logs: Spark driver eventlogs.
    returns: DataFrame of task data. One row per task with metrics.
    '''
    sql = cls._create_task_metrics_sql(event_logs_table, sql_filter=sql_filter)
    #print(sql)
    df_v1:DataFrame = cls._spark_session().sql(sql)
    df_with_extracted_task_metrics_info:DataFrame = cls._create_task_extract_metrics_df(df_v1)
    df_with_extracted_info:DataFrame = cls._create_task_extract_accumulables_df(
      df_with_extracted_task_metrics_info)
    return df_with_extracted_info
  
  @classmethod
  def create_stage_data(
    cls, 
    event_logs_table:str,  # create a view from a df if required
    sql_filter:str='') -> DataFrame:
    '''Parse event_logs and return a DataFrame of stage data.
    event_logs: Spark driver eventlogs.
    returns: DataFrame of stage data. One row per stage with metrics.
    '''
    sql:str = cls._create_stages_sql(event_logs_table, sql_filter=sql_filter)
    print(f"[create_stage_data] creating dataframe with :\n{sql}")
    return cls._spark_session().sql(sql)

  @classmethod
  def create_task_stage_data(
    cls, task_data:DataFrame, stage_data:DataFrame) -> DataFrame:
    '''Combine task_data with stage_data., calculate stage metrics and use them to calculate task outliers.
    '''
    task_data.createOrReplaceTempView('task_data')
    stage_data.createOrReplaceTempView('stage_data')
    sql:str = cls._add_stage_info_for_tasks('task_data', 'stage_data')
    print(sql)
    return cls._spark_session().sql(sql)

  @classmethod
  def add_stage_outlier_data(
    cls, task_and_stage_data:DataFrame, 
    accumulables_keys:list,
    task_metrics_keys:list) -> DataFrame:
    '''Add stage metrics for each task, and add outlier flags.
    '''
    _df:DataFrame = cls._create_sql_metrics(
      accumulables_keys=accumulables_keys, 
      task_metrics_keys=task_metrics_keys,
      #metrics_extract_table=metrics_extract_table, 
      task_and_stage_data=task_and_stage_data)
    return _df
  
  @classmethod
  def _create_outlier_data_all(cls, spark_event_log:DataFrame, materialize_suffix:str=None, sql_filter:str='') -> DataFrame:
    '''Use spark_event_log to create the full outlier data and return it.
    '''
    if not materialize_suffix:
      materialize_suffix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    print(f"materialize_suffix: {materialize_suffix}")

    event_log_view = f'event_log_view_{materialize_suffix}'
    print(f"event_log_view: {event_log_view}")
    spark_event_log.createOrReplaceTempView(event_log_view)

    df_task_data:DataFrame = cls.create_task_data(
      event_logs_table=event_log_view,
      accumulables_keys=Keys._accumulables_keys,
      task_metrics_keys=Keys._task_metrics_keys,
      sql_filter=sql_filter
    )
    print(f"df_task_data: {df_task_data}")

    # Materialize this to assist the next stage.
    task_data_table = f'task_data_{materialize_suffix}'
    print(f"materializing task_data_table: {task_data_table}")
    cls._materialize(df_task_data, task_data_table, analyze=True)
    cls._spark_session().sql(f"analyze table {task_data_table} compute statistics for columns stage_id")
    df_task_data_m:DataFrame = cls._spark_session().table(task_data_table)
    print(f"materialized task_data_table: {task_data_table}")

    df_stage_data:DataFrame = cls.create_stage_data(
      event_logs_table=event_log_view,
      sql_filter=sql_filter
    )

    # Materialize this to assist the next stage.
    stage_data_table = f'stage_data_{materialize_suffix}'
    print(f"materializing stage_data_table: {stage_data_table}")
    cls._materialize(df_stage_data, stage_data_table, analyze=True)
    cls._spark_session().sql(f"analyze table {stage_data_table} compute statistics for columns stage__id")
    df_stage_data_m:DataFrame = cls._spark_session().table(stage_data_table)
    print(f"materialized stage_data_table: {stage_data_table}")

    df_task_and_stage_data:DataFrame = cls.create_task_stage_data(
      task_data=df_task_data_m,  # the materialized task data
      stage_data=df_stage_data_m)
    
    df_outlier_data:DataFrame = cls.add_stage_outlier_data(
      task_and_stage_data=df_task_and_stage_data, 
      accumulables_keys=Keys._accumulables_keys,
      task_metrics_keys=Keys._task_metrics_keys)
    
    return df_outlier_data

  @classmethod
  def _createDataFromLogs(cls, eventlogs:List[str], cluster_id:str) -> DataFrame:
    _df:DataFrame = (cls._spark_session().read.text(eventlogs)
      .withColumnRenamed('value', 'logMessage')
      .withColumn('clusterId', lit(cluster_id)))
    return _df
  
  @classmethod
  def createDataFromLogs(cls, eventlogs:List[str], cluster_id:str, sql_filter:str='') -> str:
    event_logs_df:DataFrame = cls._createDataFromLogs(eventlogs, cluster_id)
    return cls.createDataFromEventsTable(event_logs_df, sql_filter=sql_filter)

  @classmethod
  def createDataFromEventsTable(cls, event_logs_df:DataFrame, results_table:str=None, sql_filter:str='') -> str:
    ts:str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    print(ts)
    final_result_df:DataFrame = cls._create_outlier_data_all(
      event_logs_df, materialize_suffix=ts, sql_filter=sql_filter)
    if not results_table:
      results_table = f"tasks_with_stage_info_results_{ts}"
    final_result_df.writeTo(results_table).createOrReplace()
    return results_table


# COMMAND ----------

class SparkTasksWithStageEDA:

  @staticmethod
  def _eda_ex(tables:List[str], sql_str:str, order_by:str='tbl', debug:bool=False) -> DataFrame:
    _sqls = ' union '.join([sql_str.format(tbl=x) for x in tables])
    _sqls += f" order by {order_by}"
    if debug:
      print(_sqls)
    return get_or_create_spark_session().sql(_sqls)

  @staticmethod
  def _eda_do(tables:List[str], sql_str:str, order_by:str='tbl', debug:bool=False):
    _df:DataFrame = SparkTasksWithStageEDA._eda_ex(sql_str)
    #display(_df)
    _df.display()

  @staticmethod
  def eda_all_tasks(results_tables:List[str], min_stage_tasks:int=None, debug=False) -> DataFrame:
    filter = ""
    if min_stage_tasks:
      filter = f"and stage_num_tasks >= {min_stage_tasks}"
    sql_str = """
    (with t as (
      select count(1) as total_tasks, sum(gt_p95_10) as gt_p95_10_tasks, sum(if(overall_score=0, gt_p95_10, 0)) as gt_p95_10_unexplained_tasks, 
      sum(if(overall_score > 0 and gt_p95_10 > 0, 1, 0)) as explained_tasks
      -- sum(if(gt_p95_10 > 0, overall_score, 0)) as gt_p95_10_overall_score, max(if(gt_p95_10 > 0, overall_score, 0)) as max_gt_p95_10_overall_score
      from {tbl}
      where true
      %s
    )
    select '{tbl}' as tbl, *, (gt_p95_10_tasks*100)/total_tasks as gt_p95_10_tasks_pct, (gt_p95_10_unexplained_tasks*100)/total_tasks as gt_p95_10_unexplained_tasks_pct
    from t
    )
    """ % (filter,)
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, debug=debug)

  @staticmethod  
  def eda_all_tasks_by_task_type(results_tables:List[str], min_stage_tasks:int=None, debug=False) -> DataFrame:
    filter = ""
    if min_stage_tasks:
      filter = f"and stage_num_tasks >= {min_stage_tasks}"
    sql_str = """
    (with t as (
      select task_type, count(1) as total_tasks, sum(gt_p95_10) as gt_p95_10_tasks, sum(if(overall_score=0, gt_p95_10, 0)) as gt_p95_10_unexplained_tasks, 
      sum(if(overall_score > 0 and gt_p95_10 > 0, 1, 0)) as explained_tasks
      -- sum(if(gt_p95_10 > 0, overall_score, 0)) as gt_p95_10_overall_score, max(if(gt_p95_10 > 0, overall_score, 0)) as max_gt_p95_10_overall_score
      from {tbl}
      where true
      %s
      group by task_type
    )
    select '{tbl}' as tbl, *, (gt_p95_10_tasks*100)/total_tasks as gt_p95_10_tasks_pct, (gt_p95_10_unexplained_tasks*100)/total_tasks as gt_p95_10_unexplained_tasks_pct
    from t)
    """ % (filter,)
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, debug=debug)

  @staticmethod  
  def eda_affected_stages(results_tables:List[str], order_by='tbl', debug=False) -> DataFrame:
    sql_str = """
      (with t as (
          select 
          stage__cluster_id, stage_id, stage__submission_time,
            count(1) as total_tasks, sum(gt_p95_10) as gt_p95_10_tasks, sum(if(overall_score=0, gt_p95_10, 0)) as gt_p95_10_unexplained_tasks, 
          sum(if(overall_score > 0 and gt_p95_10 > 0, 1, 0)) as explained_tasks,
          sum(task_time) as total_task_time, 
          min(task_launch_time) as min_task_launch_time, max(task_finish_time) as max_task_finish_time, 
          (max(task_finish_time) - min(task_launch_time))/(1000*60) as stage_time_m,
          percentile(task_time, 0.95) p95_task_time,
          max(task_time) as max_task_time
          -- sum(if(gt_p95_10 > 0, overall_score, 0)) as gt_p95_10_overall_score, max(if(gt_p95_10 > 0, overall_score, 0)) as max_gt_p95_10_overall_score
          from {tbl}
          group by 1, 2, 3
      ),
      t2 as (
      select -- if(gt_p95_10_tasks > 0, 'outlier task stage', 'no outlier task stage') as stage_type, 
        count(1) as total_stages,
        sum(if(gt_p95_10_tasks > 0, 1, 0)) num_affected_stages_all_outliers,
        (sum(if(gt_p95_10_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_all_outliers,
        sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0)) num_affected_stages_unexplained_outliers,
        (sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_unexplained_outliers,
        min(p95_task_time) min_p95_task_time, max(p95_task_time) max_p95_task_time, 
        min(max_task_time) min_task_time, max(max_task_time) max_task_time
      from t
      ),
      t3 as (
      select -- if(gt_p95_10_tasks > 0, 'outlier task stage', 'no outlier task stage') as stage_type, 
        count(1) as total_stages,
        sum(if(gt_p95_10_tasks > 0, 1, 0)) num_affected_stages_all_outliers,
        (sum(if(gt_p95_10_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_all_outliers,
        sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0)) num_affected_stages_unexplained_outliers,
        (sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_unexplained_outliers,
        min(p95_task_time) min_p95_task_time, max(p95_task_time) max_p95_task_time,
        min(max_task_time) min_task_time, max(max_task_time) max_task_time
      from t
      where total_tasks > 20
      ),
      t4 as (
      select -- if(gt_p95_10_tasks > 0, 'outlier task stage', 'no outlier task stage') as stage_type, 
        count(1) as total_stages,
        sum(if(gt_p95_10_tasks > 0, 1, 0)) num_affected_stages_all_outliers,
        (sum(if(gt_p95_10_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_all_outliers,
        sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0)) num_affected_stages_unexplained_outliers,
        (sum(if(gt_p95_10_unexplained_tasks > 0, 1, 0))*100)/count(1) as pct_affected_stages_unexplained_outliers,
        min(p95_task_time) min_p95_task_time, max(p95_task_time) max_p95_task_time,
        min(max_task_time) min_task_time, max(max_task_time) max_task_time
      from t
      where total_tasks > 100
      )
      select '{tbl}' as tbl, 'all_stages' as stage_class, total_stages, 
        num_affected_stages_all_outliers, (num_affected_stages_all_outliers*100)/total_stages as pct_affected_stages_all_outliers,
        num_affected_stages_unexplained_outliers, (num_affected_stages_unexplained_outliers*100)/total_stages as pct_affected_stages_unexplained_outliers 
      from t2
      union all
      select '{tbl}' as tbl, 'stages with tasks>20' as stage_class, total_stages, 
        num_affected_stages_all_outliers, (num_affected_stages_all_outliers*100)/total_stages as pct_affected_stages_all_outliers,
        num_affected_stages_unexplained_outliers, (num_affected_stages_unexplained_outliers*100)/total_stages as pct_affected_stages_unexplained_outliers 
      from t3
      union all
      select '{tbl}' as tbl, 'stages with tasks>100' as stage_class, total_stages, 
        num_affected_stages_all_outliers, (num_affected_stages_all_outliers*100)/total_stages as pct_affected_stages_all_outliers,
        num_affected_stages_unexplained_outliers, (num_affected_stages_unexplained_outliers*100)/total_stages as pct_affected_stages_unexplained_outliers 
      -- select '{tbl}' as tbl, 'stages with tasks>100' as stage_class, total_stages, num_affected_stages, (num_affected_stages*100)/total_stages as pct_affected_stages 
      from t4
      )
    """
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, order_by=order_by, debug=debug)

  @staticmethod  
  def eda_affected_stages_ranked(results_tables:List[str], order_by='tbl, gt_p95_10_unexplained_tasks desc', debug=False) -> DataFrame:
    sql_str = """
      (with t as (
          select 
          stage__cluster_id, stage_id, timestamp(stage__submission_time/1000) as stage__submission_ts,
            count(1) as total_tasks,
            sum(gt_p95_10) as gt_p95_10_tasks, 
            sum(if(overall_score=0, gt_p95_10, 0)) as gt_p95_10_unexplained_tasks, 
            sum(if(overall_score > 0 and gt_p95_10 > 0, 1, 0)) as gt_p95_10_explained_tasks,
            sum(task_time) as total_task_time, 
          min(task_launch_time) as min_task_launch_time, max(task_finish_time) as max_task_finish_time, 
          (max(task_finish_time) - min(task_launch_time))/(1000*60) as stage_time_m,
          percentile(task_time, 0.95) p95_task_time,
          max(task_time) as max_task_time
          -- sum(if(gt_p95_10 > 0, overall_score, 0)) as gt_p95_10_overall_score, max(if(gt_p95_10 > 0, overall_score, 0)) as max_gt_p95_10_overall_score
          from {tbl}
          group by 1, 2, 3
      )
      select '{tbl}' as tbl, *, (gt_p95_10_tasks*100)/total_tasks as gt_p95_10_tasks_pct, 
        (gt_p95_10_unexplained_tasks*100)/total_tasks as gt_p95_10_unexplained_tasks_pct,
        (gt_p95_10_explained_tasks*100)/total_tasks as gt_p95_10_explained_tasks_pct
      from t
      )
      """
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, order_by=order_by, debug=debug)

  @staticmethod
  def eda_top_executors(results_tables:List[str], order_by='tbl, outlier_pct desc', limit=10, debug=False) -> DataFrame:
    sql_str = """
    (
    with t_group as (
    select executor_id, host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    group by 1, 2
    ),
    t_unexplained as (
    select executor_id, host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    and gt_p95_10 > 0
    and overall_score = 0
    group by 1, 2
    ),
    t_outlier as (
    select executor_id, host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    and gt_p95_10 > 0
    -- and overall_score = 0
    group by 1, 2
    )
    select '{tbl}' as tbl, t_group.executor_id, t_group.host, 
      t_group.c as total_tasks,
      t_unexplained.c as unexplained_tasks, (t_unexplained.c*100)/t_group.c as unexplained_pct,
      t_outlier.c as outlier_tasks, (t_outlier.c*100)/t_group.c as outlier_pct,
      t_group.min_event_time as min_t_executor, t_group.max_event_time as max_t_executor, 
      t_outlier.min_event_time as min_t_outlier, t_outlier.max_event_time as max_t_outlier,
      -- below does not work with DBR 12.2
      date_diff(SECOND, t_group.min_event_time, t_group.max_event_time) as t_executor_est_duration_s,
      date_diff(HOUR, t_group.min_event_time, t_group.max_event_time) as t_executor_est_duration_H
      -- date_diff(SECOND, t_unexplained.min_event_time, t_unexplained.max_event_time) as t_unexplained_d_s,
      -- date_diff(SECOND, t_outlier.min_event_time, t_outlier.max_event_time) as t_unexplained_d_s
    from t_group 
    left outer join t_unexplained on (t_group.executor_id = t_unexplained.executor_id and t_group.host = t_unexplained.host)
    left outer join t_outlier on (t_group.executor_id = t_outlier.executor_id and t_group.host = t_outlier.host)
    order by outlier_pct desc
    limit %s
    )
    """ % (limit,)
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, order_by, debug=debug)
  
  @staticmethod
  def eda_top_vms(results_tables:List[str], order_by='tbl, outlier_pct desc', limit=10, debug=False) -> DataFrame:
    sql_str = """
    (
    with t_group as (
    select host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    group by host
    ),
    t_unexplained as (
    select host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    and gt_p95_10 > 0
    and overall_score = 0
    group by host
    ),
    t_outlier as (
    select host, count(1) as c, date_format(min(event_time), 'yyyy-MM-dd HH:mm:ss') min_event_time, date_format(max(event_time), 'yyyy-MM-dd HH:mm:ss') max_event_time
    from {tbl}
    where true
    and gt_p95_10 > 0
    -- and overall_score = 0
    group by host
    )
    select '{tbl}' as tbl, t_group.host, 
      t_group.c as total_tasks,
      t_unexplained.c as unexplained_tasks, (t_unexplained.c*100)/t_group.c as unexplained_pct,
      t_outlier.c as outlier_tasks, (t_outlier.c*100)/t_group.c as outlier_pct,
      t_group.min_event_time as min_t_executor, t_group.max_event_time as max_t_executor, 
      t_outlier.min_event_time as min_t_outlier, t_outlier.max_event_time as max_t_outlier,
      -- below does not work with DBR 12.2
      date_diff(SECOND, t_group.min_event_time, t_group.max_event_time) as t_executor_est_duration_s,
      date_diff(HOUR, t_group.min_event_time, t_group.max_event_time) as t_executor_est_duration_H
      -- date_diff(SECOND, t_unexplained.min_event_time, t_unexplained.max_event_time) as t_unexplained_d_s,
      -- date_diff(SECOND, t_outlier.min_event_time, t_outlier.max_event_time) as t_unexplained_d_s
    from t_group 
    left outer join t_unexplained on (t_group.host = t_unexplained.host)
    left outer join t_outlier on (t_group.host = t_outlier.host)
    order by outlier_pct desc
    limit %s
    )
    """ % (limit,)
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, order_by, debug=debug)

  @staticmethod
  def eda_top_outlier_tasks(results_tables:List[str], min_stage_tasks:int=None, unexplained_tasks:bool=False, limit:int=10, debug=False) -> DataFrame:
    filter = ""
    if min_stage_tasks:
      filter = f"and stage_num_tasks >= {min_stage_tasks}"
    sql_str = """
      (
      select '{tbl}' as tbl, task_id, task_time as task_time_ms, stage_p95_task_time, stage_num_tasks, timestamp(task_launch_time/1000) as task_launch_ts, stage__id, host,  overall_score as explain_metrics
      from {tbl}
      where gt_p95_10 > 0
      %s
      order by overall_score asc, task_time desc
      limit %s
      )
    """ % (filter, limit)
    return SparkTasksWithStageEDA._eda_ex(results_tables, sql_str, debug=debug)

  # probably needs to be deleted
  @staticmethod
  def _run_eda_notebook(results_tables:List[str], debug=False):
    print("All tasks")
    display(SparkTasksWithStageEDA.eda_all_tasks(results_tables))
    print("All tasks by task type")
    display(SparkTasksWithStageEDA.eda_all_tasks_by_task_type(results_tables))
    print("Affected stages")
    display(SparkTasksWithStageEDA.eda_affected_stages(results_tables))
    print("Affected stages ranked")
    display(SparkTasksWithStageEDA.eda_affected_stages_ranked(results_tables))
    print("Top Executors")
    display(SparkTasksWithStageEDA.eda_top_executors(results_tables))
    print("Top VMs")
    display(SparkTasksWithStageEDA.eda_top_vms(results_tables))
    print("Top Outlier tasks")
    display(SparkTasksWithStageEDA.eda_top_outlier_tasks(results_tables))

  @staticmethod
  def run_eda(results_tables:List[str], debug=False):
    print("All tasks")
    print(SparkTasksWithStageEDA.eda_all_tasks(results_tables, debug=debug).display())
    print("All tasks by task type")
    print(SparkTasksWithStageEDA.eda_all_tasks_by_task_type(results_tables, debug=debug).display())
    print("Affected stages")
    print(SparkTasksWithStageEDA.eda_affected_stages(results_tables, debug=debug).display())

  @staticmethod
  def run_eda_all(results_tables:List[str], debug=False):
    print("All tasks")
    print(SparkTasksWithStageEDA.eda_all_tasks(results_tables, debug=debug).display())
    print("All tasks by task type")
    print(SparkTasksWithStageEDA.eda_all_tasks_by_task_type(results_tables, debug=debug).display())
    print("Affected stages")
    print(SparkTasksWithStageEDA.eda_affected_stages(results_tables, debug=debug).display())
    print("Affected stages ranked")
    print(SparkTasksWithStageEDA.eda_affected_stages_ranked(results_tables, debug=debug).display())
    print("Top Executors")
    print(SparkTasksWithStageEDA.eda_top_executors(results_tables, debug=debug).display())
    print("Top VMs")
    print(SparkTasksWithStageEDA.eda_top_vms(results_tables, debug=debug).display())
    print("Top Outlier tasks")
    print(SparkTasksWithStageEDA.eda_top_outlier_tasks(results_tables).display())


# COMMAND ----------


