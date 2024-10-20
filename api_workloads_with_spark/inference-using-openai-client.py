# Databricks notebook source
# Keep this commented out. This notebook is a library designed to be %run from other notenbooks.
# If using this notebook directly, uncomment to run, then re-comment.

#!pip install openai
#dbutils.library.restartPython()


# COMMAND ----------

import os, sys, traceback
import openai
from openai import OpenAI
import time, datetime

# COMMAND ----------

def _get_workspace_url() -> str:
    return spark.getActiveSession().conf.get(
      "spark.databricks.workspaceUrl", None
    )


# COMMAND ----------

# This needs to run on the driver because it needs a sparksession
_DEFAULT_BASE_URL = f"https://{_get_workspace_url()}/serving-endpoints"
#print(_DEFAULT_BASE_URL)

# COMMAND ----------

# Create client class.
# The class creates the client in the initialization and uses is for every request.
# This is useful when the client is being used to make many requests, for example in a spark job.
# See the spark UDF for more on this.
from typing import TypedDict

class RequestParams(TypedDict):
    model: str
    temperature: float
    max_tokens: int

class ChatClient:

    class ClientParams(TypedDict):
        api_key: str
        base_url: str
        timeout: int
        max_retries: int

    _DEFAULT_MODEL = "databricks-dbrx-instruct"
    _DEFAULT_SYSTEM_PROMPT = "You are an helpful assistant"

    _DEFAULT_REQUEST_PARAMS = {
        'model': _DEFAULT_MODEL,
        'temperature': 0,
        'max_tokens': 1024
    }

    _DEFAULT_TIMEOUT = 10  # 10 seconds
    _DEFAULT_MAX_TRIES = 3

    def _init_client(self, client_params:ClientParams):
        self.client = OpenAI(
            api_key=client_params.get('api_key'),
            base_url=client_params.get('base_url'),
            timeout=client_params.get('timeout'),
            max_retries=client_params.get('max_retries')
        )

    def __init__(
        self,
        api_key:str,
        base_url:str=_DEFAULT_BASE_URL,
        timeout:float=_DEFAULT_TIMEOUT,
        max_retries:int=_DEFAULT_MAX_TRIES, 
        system_prompt:str=_DEFAULT_SYSTEM_PROMPT,
        default_request_params:RequestParams=_DEFAULT_REQUEST_PARAMS):

        self.client_params:ChatClient.ClientParams = {
            'api_key': api_key,
            'base_url': base_url,
            'timeout': timeout,
            'max_retries': max_retries
        }
        self.system_prompt=system_prompt
        self.default_request_params=default_request_params
        self._init_client(self.client_params)

    def _create_request_params(self, rp:RequestParams) -> RequestParams:
        _rp = {
            'model': rp.get('model') if rp.get('model') is not None else self.default_request_params.get('model'),
            'temperature': rp.get('temperature') if rp.get('temperature') is not None else self.default_request_params.get('temperature'),
            'max_tokens': rp.get('max_tokens') if rp.get('max_tokens') is not None else self.default_request_params.get('max_tokens')
        }
        return _rp

    def query(self, q:str=None, messages:list=None, request_params:RequestParams={}) -> tuple:
        '''Make a single query of a model.
        q: The query. Use q or messages, not both.
        messages: A list of messages. If this is specified, it should include a system prompt. self.system_prompt will not be used.
        request_params: keys: ['model', 'temperature', 'max_tokens']
        '''
        if not q and not messages:
            raise Exception("Must specify one of q or messages")
        if q and messages:
            raise Exception("Specify one of q or messages, not both")
        if q:
            messages=[
                {
                    "role": "system",
                    "content": self.system_prompt
                },
                {
                    "role": "user",
                    "content": q
                }
            ]

        _request_params:RequestParams = self._create_request_params(request_params)
        print(f"Final request params: {_request_params}")
        print(f"final messages: {messages}")

        request_start_time = time.time()
        try:
            completions = self.client.chat.completions.create(
                messages=messages,
                model=_request_params['model'],
                temperature=_request_params["temperature"],
                max_tokens=_request_params["max_tokens"]
            )

            latency = time.time() - request_start_time
            
            return (
                #completions.choices[0].text, # if using finetuned model
                completions.choices[0].message.content, # if using chat model
                completions.usage.completion_tokens,
                completions.usage.prompt_tokens,
                completions.usage.total_tokens,
                latency,
                None,
                'ok'
            )

        except Exception as e:
            traceback.print_exc()
            return None, 0, 0, 0, 0.0, str(e), 'error'
    
    # This is simpler to call since it just returns a string
    def query2(self, q:str=None, messages:list=None, request_params:RequestParams={}) -> str:
        r = self.query(q=q, messages=messages, request_params=request_params)
        if r[-1] != 'ok':
            return None
        return r[0]

    def __str__(self):
        return f"ChatClient(system_prompt={self.system_prompt}, default_request_params={self.default_request_params})"
    
    # Below enable ChatClient to be pickled. The OpenAI client cannot be pickled. So this technique removes the client before
    # pickling, and reinitializes the client after unpickling.
    def __getstate__(self):
        state = self.__dict__.copy()
        # Do not pickle the client itself
        state['client'] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Reinitialize the client after unpickling
        self._init_client(self.client_params)    

# COMMAND ----------

class InferenceTest:
  def __init__(self):
    self.token = dbutils.secrets.get(scope='nishant-deshpande', key='api_token')
    self.url = "https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints"
    self.model = "databricks-dbrx-instruct"
    self.system_prompt = "You are an helpful assistant"
    self.default_request_params = {
      'model': self.model,
      'temperature': 0,
      'max_tokens': 1024
    }

  def test_openai_client(self):
    print("-- test_openai_client --")
    client = OpenAI(
        api_key=self.token,
        base_url=self.url
    )
    response = client.chat.completions.create(
        model=self.model,
        messages=[
          {
            "role": "system",
            "content": self.system_prompt
          },
          {
            "role": "user",
            "content": "Explain what type of model you are.",
          }
        ],
        max_tokens=1024
    )
    print(f"response: {response}\n")
    print(f"default content: {response.choices[0].message.content}")
  
  def test_chat_client(self):
    print("-- test_chat_client --")
    cc = ChatClient(api_key=self.token)
    _r = cc.query(q="What type of model are you?")
    print(f"query response: {_r}")
    print("\n")
    _r = cc.query2(q="What type of model are you?")
    print(f"query2 response: {_r}")

  def test_chat_client_params(self):
    print("-- test_chat_client_params --")
    cc = ChatClient(
      api_key=self.token,
      system_prompt="You are a poet. Please answer with a haiku and sign it with your model type.")
    _r = cc.query(q="What type of model are you?", request_params={'model': 'databricks-meta-llama-3-1-405b-instruct'})
    print(f"query response: {_r}")
    print("\n")
    _r = cc.query2(q="What type of model are you?", request_params={'model': 'databricks-meta-llama-3-1-405b-instruct'})
    print(f"query2 response: {_r}")

  def test_chat_client_params2(self):
    chat_haiku_client = ChatClient(
      api_key=self.token, #timeout=30, 
      system_prompt="You are a poet. Please answer with a haiku and sign it with your model type.",
      default_request_params={'model':'databricks-meta-llama-3-1-405b-instruct'})
    _r = chat_haiku_client.query(q="What type of model are you?", request_params={'model': 'databricks-meta-llama-3-1-405b-instruct'})
    print(f"query response: {_r}")
    print("\n")
  
  def do_test(self):
    self.test_openai_client()
    print("\n\n")
    self.test_chat_client()
    print("\n\n")
    self.test_chat_client_params()
    print("\n\n")
    self.test_chat_client_params2()

# COMMAND ----------

# Keep commented. Use for testing this module only.
#InferenceTest().do_test()
#InferenceTest().test_chat_client_params2()

# COMMAND ----------


