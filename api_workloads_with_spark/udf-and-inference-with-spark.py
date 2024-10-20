# Databricks notebook source
# MAGIC %md
# MAGIC ### Inference with Spark - basics
# MAGIC
# MAGIC This notebook illustrates how to do inference (call a language model) with spark using UDFs.
# MAGIC
# MAGIC It uses the ChatClient developed in the notebook inference-using-chatgpt-client
# MAGIC

# COMMAND ----------

# plain UDFs: https://docs.databricks.com/en/udf/python.html
# pandas udfs: https://docs.databricks.com/en/udf/pandas.html

# COMMAND ----------

!pip install openai
dbutils.library.restartPython()


# COMMAND ----------

# MAGIC %run ./inference-using-openai-client

# COMMAND ----------

# Note: creating a variable with the token in the driver is required
# if the openai client is used in spark workers.
# The variable is accessible to the spark workers via closure.
DATABRICKS_TOKEN = dbutils.secrets.get(scope='nishant-deshpande', key='api_token')

# If running a python process locally, the OPENAI_API_KEY environment variable will work.
# Environment variables do not get recreated in workers.
#os.environ['OPENAI_API_KEY'] = DATABRICKS_TOKEN


# COMMAND ----------

# For inference with a client, we will use a pandas udf with an Iterator.
# This allows the client to be initialized once per batch.
# This may not be all that important for inference / REST API calls since almost all the time is taken in the inference api call,
# but this is good practice anyway.

# COMMAND ----------

from typing import Iterator
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
#from pyspark.sql.functions import pandas_udf, col
import pandas as pd
import json

# COMMAND ----------

# Create a small dataset to test
questions = open('100_questions_about_databricks_technology.txt', 'r').readlines()
questions = [question.strip() for question in questions]

df = spark.createDataFrame(questions, StringType()).toDF('question').limit(12).repartition(4)

display(df)


# COMMAND ----------

# Let's use ChatClient.query2 since it returns a string. ChatClient.query returns a tuple, and we will see how to use that later.
# Let's specify some non-default parameters to ChatClient and use them.
# The global parameters specified here will be created in every worker by closure of the python UDF.
SYSTEM_PROMPT = "You are a poet. Please answer with a haiku and sign it with your model type."
DEFAULT_REQUEST_PARAMS = {'model':'databricks-meta-llama-3-1-405b-instruct'}

# COMMAND ----------

@F.pandas_udf("string")
def chat_client_query2_pandas_udf(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  cc = ChatClient(
    api_key=DATABRICKS_TOKEN, system_prompt=SYSTEM_PROMPT, 
    default_request_params=DEFAULT_REQUEST_PARAMS)
  for data in iter:
    yield data.apply(lambda x: cc.query2(q=x))


# COMMAND ----------

display(df.withColumn('answer', chat_client_query2_pandas_udf(F.col('question'))))

# COMMAND ----------

# We can create the ChatClient here, and have it auto-magically serialized to all the workers.
# Note: See the special technique to enable ChatClient to be pickled in the class definition.
# This is because the client:OpenAI object in ChatClient cannot be pickled. 
chat_haiku_client = ChatClient(
  api_key=DATABRICKS_TOKEN, #timeout=30, 
  system_prompt="You are a poet. Please answer with a haiku and sign it with your model type.",
  default_request_params={'model':'databricks-meta-llama-3-1-405b-instruct'})

# COMMAND ----------

@F.pandas_udf("string")
def chat_client_haiku_query2_pandas_udf(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  for data in iter:
    yield data.apply(lambda x: chat_haiku_client.query2(q=x))


# COMMAND ----------

display(df.withColumn('answer', chat_client_haiku_query2_pandas_udf(F.col('question'))))

# COMMAND ----------



# COMMAND ----------

# We have seen how to use a parameterized ChatClient in workers.
# Now lets use ChatClient.query.
# This returns a struct as the response.
# ChatClient.query could return a json string to make it simple for us.
# But lets learn to return a struct from a UDF.



# COMMAND ----------

from typing import List, Dict, Any, Tuple, Optional, TypedDict
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, FloatType

udf_returntype =StructType(
    [
        StructField('content', StringType(), nullable=True),
        StructField('completion_tokens', IntegerType(), nullable=True),
        StructField('prompt_tokens', IntegerType(), nullable=True),
        StructField('total_tokens', IntegerType(), nullable=True),
        StructField('latency', FloatType(), nullable=True),
        StructField('error_string', StringType(), nullable=True),
        StructField('status', StringType(), nullable=True)
    ]
)
print(udf_returntype)


# COMMAND ----------

# Let's start with a non-pandas UDF.
@F.udf(udf_returntype)
def chat_client_query_udf(question:str) -> tuple:
  cc = ChatClient(api_key=DATABRICKS_TOKEN)
  return cc.query(question)


# COMMAND ----------

# Note the returned Dataframe schema. The answer column is a struct.
_r = df.withColumn('answer', chat_client_query_udf(F.col('question')))

# COMMAND ----------

display(_r)

# COMMAND ----------



# COMMAND ----------

# Now try a pandas udf so we can use arrow data exchange format, batch the data, and initialize the client once.
# Given the client is calling an LLM, initializing the (probably very lightweight) client once probably doesn't matter here.

# COMMAND ----------

@F.pandas_udf(udf_returntype)
#@pandas_udf("output string, completion_tokens int, prompt_tokens int, total_tokens int, latency float, error string")
def chat_client_query_pandas_udf_v2(content_batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    cc = ChatClient(api_key=DATABRICKS_TOKEN)
    for content_batch in content_batches:
        yield pd.DataFrame.from_records(content_batch.apply(cc.query))



# COMMAND ----------

# Note the returned Dataframe schema. The answer column is a struct.
_r = df.withColumn('answer', chat_client_query_pandas_udf_v2(F.col('question')))

# COMMAND ----------

display(_r)

# COMMAND ----------



# COMMAND ----------

# Specifying the return schema with a string like below also works.
@F.pandas_udf("output string, completion_tokens int, prompt_tokens int, total_tokens int, latency float, error string, status string")
def chat_client_query_pandas_udf_v3(content_batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    cc = ChatClient(api_key=DATABRICKS_TOKEN)
    for content_batch in content_batches:
        yield pd.DataFrame.from_records(content_batch.apply(cc.query))


# COMMAND ----------

# Note the returned Dataframe schema. The answer column is a struct.
_r = df.withColumn('answer', chat_client_query_pandas_udf_v2(F.col('question')))

# COMMAND ----------

display(_r)

# COMMAND ----------



# COMMAND ----------

# If you prefer a json rather than a struct, wrap the ChatClient.query with a json converter,
# and use that in the UDF.

# COMMAND ----------

def t_to_json(t:tuple) -> str:
  return json.dumps({
    'content':t[0], 'completion_tokens':t[1], 'prompt_tokens':t[2], 'total_tokens':t[3], 'latency':t[4], 'error_string':t[5], 'status':t[6]
    })

@F.pandas_udf("string")
def chat_client_query_pandas_udf(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
  cc = ChatClient(api_key=DATABRICKS_TOKEN)
  for data in iter:
    yield data.apply(lambda x: t_to_json(cc.query(q=x)))


# COMMAND ----------

# Note the answer column is now a json string.
_r = df.withColumn('answer', chat_client_query_pandas_udf(F.col('question')))

# COMMAND ----------

display(_r)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


