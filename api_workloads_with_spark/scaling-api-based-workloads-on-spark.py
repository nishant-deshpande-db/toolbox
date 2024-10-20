# Databricks notebook source
# MAGIC %md
# MAGIC ## Scaling API based workloads on Spark
# MAGIC
# MAGIC Workloads for LLM inference are increasing. Generically, these may be termed model inference workloads.
# MAGIC
# MAGIC Model inference can use a local model, or (especially with large LLMs or using 3rd party LLMs as a service) via an API / endpoint.
# MAGIC
# MAGIC This notebook looks at how to efficiently run API/endpoint workloads using spark.
# MAGIC
# MAGIC [A different notebook] will look at running local workloads efficiently.
# MAGIC
# MAGIC ### API based workloads on spark
# MAGIC
# MAGIC These workloads spend most of their time waiting for the API response. So we want to:
# MAGIC  1. Use the spark cluster efficiently to generate as many concurrent requests as the endpoint will support.
# MAGIC  2. If the API we are using has an unknown concurrent request capacity, figure out the capacity.
# MAGIC
# MAGIC ##### 1. Use the spark cluster efficiently to generate as many concurrent requests as we want to
# MAGIC
# MAGIC To get R concurrent requests, set SPARK_WORKER_CORES = R / number_of_workers.
# MAGIC
# MAGIC SPARK_WORKER_CORES tells the driver how many cores each worker has. This can be more than the actual number of cores.
# MAGIC
# MAGIC For example, if you want 200 concurrent requests, you need to have 200 concurrent tasks. If you have a spark cluster with 2 nodes with 4 vcpus each, to get 200 concurrent tasks, set SPARK_WORKER_CORES = 200 / 2 = 100. This will mean 100/4 = 25 tasks will run concurrently on each vcpu.
# MAGIC
# MAGIC ##### Why use spark at all?
# MAGIC
# MAGIC 1. If you're using spark to read/write data (from permissioned tables, using structured streaming,..), it is easier to stick to spark.
# MAGIC 2. You could get 20+ concurrent requests per vcpu, and so have 100s of concurrent request with a 16 vcpu machine. You could do this via some other engine (python MultiProcess etc), but you'd have to do some work to read the data, write the results, handle errors etc.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC %pip install openai
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

import typing
import pyspark


# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

utils = InferenceUdfScalingEndToEnd_Utils()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create a cluster that can do 200 concurrent requests.
# MAGIC For example, 2 workers with 8 vcpus each. Set SPARK_WORKER_CORES = 100.
# MAGIC
# MAGIC This will create 100 python processes on each worker, which should be comfortable for 8 vcpus. You can normally do 20+ "concurrent" tasks per vcpu (for API style tasks) as mentioned earlier. But here we're trying to get the API/endpoint throughput, not what the spark cluster can do. So lets make sure the client making the requests isn't the bottleneck! 
# MAGIC
# MAGIC Attach this notebook to the cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Create dataset
# MAGIC Create a dataset that will generate API traffic for 5 minutes at 200 qps.
# MAGIC

# COMMAND ----------

num_records = 200 * 5 * 60
print(f"Estimated number of records required: {num_records}")

# COMMAND ----------

import pyspark.sql.types as T

questions = open('100_questions_about_databricks_technology.txt', 'r').readlines()
questions = [question.strip() for question in questions]
print(len(questions))
_m = int(num_records/len(questions))
df = spark.createDataFrame(questions, T.StringType()).toDF('question')
TEST_DATASET_df = df.crossJoin(spark.range(_m)).select('question')  # .cache().  # .cache() doesn't work in serverless right now
print(TEST_DATASET_df.count())
# df is a dataframe with 60,000 records.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Check the spark cluster is doing what you think it is doing.
# MAGIC
# MAGIC Before testing the throughput of the endpoint, lets do some cursory check that our concurrency is what is expected.
# MAGIC
# MAGIC Lets do a simple test.
# MAGIC

# COMMAND ----------

import time, datetime
import pyspark.sql.types as T
import pyspark.sql.functions as F

def sleep_fn(value: int) -> int:
    if value <= 0: return value
    #print(f'[{datetime.datetime.now()}] sleeping {value} seconds')
    time.sleep(value)
    #print(f'[{datetime.datetime.now()}] awake')
    return value

sleep_udf = F.udf(sleep_fn, T.IntegerType())


# COMMAND ----------

import traceback
# Turn off any flags that might interfere in partitioning the data as required!
for c in ['spark.databricks.optimizer.adaptive.enabled',
          'spark.databricks.delta.autoCompact.enabled',
          'spark.databricks.delta.optimizeWrite.enabled']:
  try:
    print(spark.conf.get(c))
    spark.conf.set(c, 'false')
    print(spark.conf.get(c))
  except:
    traceback.print_exc()


# COMMAND ----------

def do_test():
  _expected_concurrency = 4
  _test_seconds = 30
  df = spark.range(_expected_concurrency * _test_seconds).withColumn('sleep_secs', F.lit(1))  # withColumn('sleep', sleep_udf(F.col('id'))).write.mode)
  #display(df)

  start_ts = time.time()
  df = df.repartition(_expected_concurrency).withColumn('ret', sleep_udf(F.col('sleep_secs')))
  df.write.mode('overwrite').parquet(f'dbfs:/tmp/nishant/tmp/{time.time()}/')
  end_ts = time.time()

  print(f'Time taken: {end_ts - start_ts} seconds')

#do_test()
# Check the time taken is as expected (approximately _test_seconds).
# Spark has some overhead and we have a write, so a few extra seconds should be expected.

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Provision an appropriate endpoint.

# COMMAND ----------

# https://docs.databricks.com/en/machine-learning/model-serving/create-foundation-model-endpoints.html
import mlflow.deployments
from mlflow.deployments.databricks import DatabricksDeploymentClient

client:DatabricksDeploymentClient = mlflow.deployments.get_deploy_client("databricks")


# COMMAND ----------

# If you already have an endpoint, set the ENDPOINT_NAME below.
# If not, create it by uncommenting the call to check_create_endpoint.
# See the function in ./utils to configure the new endpoint as required.

ENDPOINT_NAME = "nishant-llama-test"
#ENDPOINT_NAME = "nishant-test-llama-test-2"

utils.check_create_endpoint(client, ENDPOINT_NAME, wait_for_ready=True)

# COMMAND ----------

# Test the endpoint. If the endpoint has scaled down to zero, this might take some time.
# Make sure this works before proceeding. Otherwise the throughput measurements will be invalid.
utils.simple_query_endpoint(client, ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Run on your actual dataset + your actual API.
# MAGIC In this case, I'm going to run on a foundation model endpoint, using ChatClient.
# MAGIC
# MAGIC See notebook udf-and-inference-with-spark for detailed explanation of the code below.

# COMMAND ----------

# MAGIC %run ./inference-using-openai-client

# COMMAND ----------

DATABRICKS_TOKEN = dbutils.secrets.get(scope='nishant-deshpande', key='api_token')

# COMMAND ----------

test_client = ChatClient(
  api_key=DATABRICKS_TOKEN, 
  system_prompt="You are a poet. Please answer with a haiku and sign it with your model type.",
  default_request_params={'model':ENDPOINT_NAME})


# COMMAND ----------

# Test the client.
test_client.query("What is the meaning of life?")

# COMMAND ----------

from typing import Iterator
from pyspark.sql.functions import pandas_udf, col
import pandas as pd

api_client = test_client

@pandas_udf("output string, completion_tokens int, prompt_tokens int, total_tokens int, latency float, error string, status string")
def chat_client_udf(content_batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    for content_batch in content_batches:
        yield pd.DataFrame.from_records(content_batch.apply(api_client.query))

# COMMAND ----------

# Create a table for the results and verify the UDF works for the test
RESULTS_TABLE = 'nishant_deshpande.genai.scaling_api_endpoints_output'

# COMMAND ----------

# Utility function to get stats from the results table.
# Filter using the label for a particular test.
def _get_stats(results_table, label=None) -> list:
  _df = spark.table(results_table)
  if label:
    _df = _df.filter(f"test_label = '{label}'")
  _df = _df.groupBy(["test_label", "answer.status"]).count()
  status_counts = [(row['test_label'], row['status'], row['count']) for row in _df.collect()]
  return status_counts


# COMMAND ----------


# Runs the spark job on dataset_df with concurrency.
# Writes to results table with a label.
# Return the wall-clock time and the stats for this test.
def throughput_test(
  dataset_df:pyspark.sql.DataFrame,
  concurrency: int, label:str=None, results_table:str=RESULTS_TABLE) -> tuple:
  if not label: label = f"[{datetime.datetime.now()}] concurrency={concurrency}"
  #print(label)
  start_ts = time.time()
  (dataset_df.repartition(concurrency)
    .withColumn('test_label', F.lit(label))
    .withColumn('answer', chat_client_udf('question'))
    .write.mode('append').saveAsTable(results_table)
  )
  end_ts = time.time()
  status_counts_dict = _get_stats(results_table, label)
  return {'run_time': end_ts - start_ts, 'run_details':status_counts_dict}



# COMMAND ----------

# test
_r = throughput_test(dataset_df=TEST_DATASET_df.limit(20), concurrency=5)
print(_r)

# COMMAND ----------

# If everything looks good, it is time to run the actual test to work out throughput of your endpoint.

# COMMAND ----------

test_concurrency = [40, 80, 100, 120, 140, 160]
_test_dataset = TEST_DATASET_df.limit(600)
for concurrency in test_concurrency:
  _r = throughput_test(dataset_df=_test_dataset, concurrency=concurrency)
  throughput = _r
  print(f"concurrency:{concurrency} {_r}\n")

# COMMAND ----------



# COMMAND ----------


