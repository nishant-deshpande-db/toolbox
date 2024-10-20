# Databricks notebook source
# Keep this commented out. This notebook is designed to be "imported" into others.
# Uncomment to test.

#%pip install mlflow
#dbutils.library.restartPython()


# COMMAND ----------

import mlflow.deployments
from mlflow.deployments.databricks import DatabricksDeploymentClient

import traceback
from mlflow.deployments.databricks import DatabricksEndpoint

import time, datetime

# COMMAND ----------

import traceback

class InferenceUdfScalingEndToEnd_Utils:
  _model = "system.ai.meta_llama_3_8b_instruct"
  _entity_name = _model.replace('.', '_')
  DEFAULT_MODEL_CONFIG = {
        "served_entities": [
          {
            "name": _entity_name,
            "entity_name": _model,
            "entity_version": "1",
            "min_provisioned_throughput":0,
            "max_provisioned_throughput":3600,
            #"workload_size": "Small",
            "scale_to_zero_enabled": True
          }
        ],
        "traffic_config": {
          "routes": [
            {
              "served_model_name": _entity_name,
              "traffic_percentage": 100
            }
          ]
        }
    }

  @staticmethod
  def _is_ready(
    client:DatabricksDeploymentClient, endpoint_name:str) -> bool:
    endp = client.get_endpoint(endpoint_name)
    ready_state = endp.get('state', {}).get('ready')
    return ready_state == "READY"

  @staticmethod
  def simple_query_endpoint(client:DatabricksDeploymentClient, endpoint_name:str) -> str:
    r = client.predict(
      endpoint=endpoint_name,
      inputs={"messages":[{"role":"user", "content":"What type of model are you?"}]}
    )
    return r['choices'][0]['message']['content']

  @staticmethod
  def check_create_endpoint(
    client:DatabricksDeploymentClient, endpoint_name:str, 
    config:dict=DEFAULT_MODEL_CONFIG,
    wait_for_ready:bool=False,
    time_limit_seconds=15*60) -> DatabricksEndpoint:
    try:
      endp = client.get_endpoint(endpoint_name)
      if endp:
        print(f"got endpoint: {endp}")
        #return endp
    except Exception as e:
      print(f"No endpoint {endpoint_name} found. Creating an endpoint.")
      endp = client.create_endpoint(endpoint_name, config)
      print(f"created endpoint: {endp}")

    start_ts = datetime.datetime.now()
    if wait_for_ready:
      while not InferenceUdfScalingEndToEnd_Utils._is_ready(client, endpoint_name):
        print(f"[{datetime.datetime.now()}] Waiting for endpoint to be ready...")
        time.sleep(5)

      # Sometimes even after the endpoint is ready, the API call to predict still fails for a few seconds.
      while True:
        try:
          InferenceUdfScalingEndToEnd_Utils.simple_query_endpoint(client, "databricks-dbrx-instruct")
          break
        except:
          traceback.print_exc()
          print("will try again in 5 seconds....")
          time.sleep(5)

    return endp



# COMMAND ----------

import copy

class InferenceUdfScalingEndToEnd_Utils_Test:
  @staticmethod
  def _test_simple_query_endpoint():
    client:DatabricksDeploymentClient = mlflow.deployments.get_deploy_client("databricks")
    _r = InferenceUdfScalingEndToEnd_Utils.simple_query_endpoint(client, "databricks-dbrx-instruct")
    print(f"simple_query_endpoint {_r}")

  @staticmethod
  def _test_check_create_endpoint(wait_for_ready=False):
    client:DatabricksDeploymentClient = mlflow.deployments.get_deploy_client("databricks")
    _c = copy.deepcopy(InferenceUdfScalingEndToEnd_Utils.DEFAULT_MODEL_CONFIG)
    _c['served_entities'][0]['max_provisioned_throughput'] = 7200
    _e = InferenceUdfScalingEndToEnd_Utils.check_create_endpoint(
      client, "nishant-test-llama-test-2",
      config=_c,
      wait_for_ready=wait_for_ready
    )
    return _e

  @staticmethod
  def _test():
    InferenceUdfScalingEndToEnd_Utils_Test._test_simple_query_endpoint()    
    endp = InferenceUdfScalingEndToEnd_Utils_Test._test_check_create_endpoint()
    print(f"Got endpoint: {endp}")

# COMMAND ----------

#InferenceUdfScalingEndToEnd_Utils_Test._test()

# COMMAND ----------

#endp = InferenceUdfScalingEndToEnd_Utils_Test._test_check_create_endpoint()

# COMMAND ----------

#endp = InferenceUdfScalingEndToEnd_Utils_Test._test_check_create_endpoint(wait_for_ready=True)

# COMMAND ----------


