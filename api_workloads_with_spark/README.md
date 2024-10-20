## Model Inference and scaling on Databricks

A set of notebooks to illustrate how to run model inference on the Databricks platform.

Notebooks in this repo:

##### [1. inference-using-openai-client](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/4251323330573501)

 - Create an OpenAI client wrapper. Test.
 - [TODO] OpenAI has parameters to customize retries. Add them to the API.

#### [2. udf-and-inference-with-spark](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/4251323330638003)

 - Use the client wrapper from (1) in a UDF. Both python and pandas UDFs, returning strings and structs.
 - Read and write spark dataframes using the UDFs.

#### [3. Scaling API based workload](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#notebook/973044544439813)

 - Scaling to get efficient inference for API calls (for example to LLMs).
 - Why use spark for API call inference?
 - Finding the highest throughput of the API (for example an LLM or vector search endpoint) using SPARK_WORKER_CORES on a spark cluster.



