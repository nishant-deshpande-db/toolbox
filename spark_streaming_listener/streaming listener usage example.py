# Databricks notebook source
import sys, os, datetime, importlib

# COMMAND ----------

import streaming_listener

# COMMAND ----------

importlib.reload(streaming_listener)

# COMMAND ----------

streaming_listener.MyListener.__VERSION__

# COMMAND ----------

ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
print(ts)

# COMMAND ----------

# On a single user / dedicated cluster, can use dbfs or volumes.
# If on a shared / standard cluster, must use volumes. Dbfs will not work as there is no direct write to dbfs.
# The listner can also write to non-mounted local filesystems, but only single-user/dedicated clusters will be able to read those files. Shared/standard and serverless will not.
BASE_DIR = '/Volumes/nishant_deshpande/data/data'
#BASE_DIR = '/dbfs'
#BASE_DIR = '/tmp'

# COMMAND ----------

# Check this cluster can write to your BASE_DIR
# If you get an error here, change your BASE_DIR.
def test_write(d:str):
  with open(f'{d}/tmp/test_{ts}', 'w') as f:
    f.write(ts)
  with open(f'{d}/tmp/test_{ts}', 'r') as f:
    print(f.read())
  

test_write(BASE_DIR)

# COMMAND ----------

# Feel free to change this as required.
event_write_path = f"{BASE_DIR}/tmp/streaming_events/{ts}"
print(event_write_path)

# COMMAND ----------

os.makedirs(event_write_path)

# COMMAND ----------

my_listener = None

# COMMAND ----------

try:
  print(f"removing any existing {my_listener}")
  spark.streams.removeListener(my_listener)
except:
  pass

# COMMAND ----------

# Create the listener.
my_listener = streaming_listener.MyListener(
  writeProgressEventsPath=event_write_path, 
  writeEveryNEvents=5)


# COMMAND ----------

# This listener will listen to all spark streams running on this cluster.
spark.streams.addListener(my_listener)


# COMMAND ----------

# Create rate source stream named "stream1"
df = (spark.readStream
      #.name("stream1")
      .format("rate").option("rowsPerSecond", 20).load()
)

# Apply a no-op foreachBatch sink.
# But do something with the df otherwise no rows will actually be processed.
query = df.writeStream \
    .queryName("stream1") \
    .foreachBatch(lambda df, epoch_id: print(f"{epoch_id}:{df.count()}")) \
    .trigger(processingTime='5 seconds').start()

#query.awaitTermination()


# COMMAND ----------

# Create rate source stream named "stream2"
df = (spark.readStream
      #.name("stream1")
      .format("rate").option("rowsPerSecond", 5).load()
)

# Apply a no-op foreachBatch sink
# But do something with the df otherwise no rows will actually be processed.
query = df.writeStream \
    .queryName("stream2") \
    .foreachBatch(lambda df, epoch_id: print(f"{epoch_id}:{df.count()}")) \
    .trigger(processingTime='8 seconds').start()

#query.awaitTermination()


# COMMAND ----------

# Check the files are being produced
for f in os.listdir(event_write_path):
  print(event_write_path + '/' + f)

# COMMAND ----------



# COMMAND ----------

# Adjust the event_write_path (which is local) to be readable by spark if it is on dbfs or local.
# NOTE: Only single-user / dedicated clusters can read local files using (file:).
spark_path = event_write_path
if event_write_path.startswith('/dbfs'):
    spark_path = event_write_path.replace('/dbfs', 'dbfs:')
elif (event_write_path.startswith('/') and not event_write_path.startswith('/Volumes')):
    spark_path = 'file:' + event_write_path
print(spark_path)


# COMMAND ----------

# Note that if files are continually written, this command might take a while to return.
# Stop the streams if that happens.
df = (spark.read.format('json')
      .option('ignoreCorruptFiles', 'true')
      .option('ignoreMissingFiles', 'true')
      .load(spark_path)
  )

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df order by timestamp desc

# COMMAND ----------

