import json
import time
import logging

logger = logging.getLogger(__name__)

from pyspark.sql.streaming import StreamingQueryListener


class MyListener(StreamingQueryListener):
  __VERSION__ = '0.2'
  def __init__(self, writeProgressEventsPath=None, writeEveryNEvents=100):
    self.writeProgressEventsPath = writeProgressEventsPath
    #if self.writeProgressEventsPath:
    #  dbutils.fs.mkdirs(f"dbfs:{self.writeProgressEventsPath}")
    #  print(f'created dbfs:{self.writeProgressEventsPath}')
    self.writeEveryNEvents = writeEveryNEvents
    self.startedEvents = []
    self.progressEvents = []
    self.terminatedEvents = []
    self.counter = 0
    self._writeProgressEventsCtr = 0
    print('MyListener initialized')

  def onQueryStarted(self, event):
    self.startedEvents.append(('queryStarted', event))
    self.counter += 1
    
  def onQueryProgress(self, event):
    self.progressEvents.append(('progress', event))
    self.counter += 1
    print(f"self.counter = {self.counter}")
    print(f"self.writeEveryNEvents = {self.writeEveryNEvents}")    
    if self.writeProgressEventsPath and self.counter % self.writeEveryNEvents == 0:
      #self.incrWriteProgressEvents(f"/dbfs{self.writeProgressEventsPath}")
      self.incrWriteProgressEvents(f"{self.writeProgressEventsPath}")
    
  def onQueryTerminated(self, event):
    self.terminatedEvents.append(('terminated', event))
    self.counter += 1
    #self.incrWriteProgressEvents(f"/dbfs{self.writeProgressEventsPath}")
    self.incrWriteProgressEvents(f"{self.writeProgressEventsPath}")
  
  def getCounts(self):
    return {'startedEvents': len(self.startedEvents),
            'progressEvents': len(self.progressEvents),
            'terminatedEvents': len(self.terminatedEvents),
            'counter': self.counter}

  def incrWriteProgressEvents(self, path):
    print(f"incrWriteProgressEvents({locals()}) CALLED")
    ctr = self._writeProgressEventsCtr
    for e in self.progressEvents[self._writeProgressEventsCtr:]:
      x = json.loads(e[1].progress.json)
      print(f"x: {x}")
      js_f = f"{path}/{x['id']}_{x['batchId']}_{x['timestamp'].replace(':','-')}.json"
      print(js_f)
      with open(js_f, 'w') as f:
        json.dump(x, f)
      ctr += 1
    self._writeProgressEventsCtr = ctr

