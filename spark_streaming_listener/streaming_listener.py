import json
import time
import logging

logger = logging.getLogger(__name__)

from pyspark.sql.streaming import StreamingQueryListener


class MyListener(StreamingQueryListener):
  __VERSION__ = '0.3'

  def __init__(self, writeProgressEventsPath=None, writeEveryNEvents=100):
    self.writeProgressEventsPath = writeProgressEventsPath
    #if self.writeProgressEventsPath:
    #  dbutils.fs.mkdirs(f"dbfs:{self.writeProgressEventsPath}")
    #  logger.info(f'created dbfs:{self.writeProgressEventsPath}')
    self.writeEveryNEvents = writeEveryNEvents
    self.startedEvents = []
    self.progressEvents = []
    self.terminatedEvents = []
    self.counter = 0
    self._writeProgressEventsCtr = 0
    logger.info('MyListener initialized')

  def onQueryStarted(self, event):
    self.startedEvents.append(('queryStarted', event))
    self.counter += 1
    
  def onQueryProgress(self, event):
    self.progressEvents.append(('progress', event))
    self.counter += 1
    logger.debug(f"self.counter = {self.counter}")
    logger.debug(f"self.writeEveryNEvents = {self.writeEveryNEvents}")    
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
    logger.debug(f"incrWriteProgressEvents({locals()}) CALLED")
    ctr = self._writeProgressEventsCtr
    for e in self.progressEvents[self._writeProgressEventsCtr:]:
      x = json.loads(e[1].progress.json)
      logger.debug(f"x: {x}")
      js_f = f"{path}/{x['id']}_{x['batchId']}_{x['timestamp'].replace(':','-')}.json"
      logger.debug(js_f)
      with open(js_f, 'w') as f:
        json.dump(x, f)
      ctr += 1
    logger.info(f"Wrote {ctr - self._writeProgressEventsCtr} events in individual files")
    logger.debug(f"self._writeProgressEventsCtr = {self._writeProgressEventsCtr}")
    self._writeProgressEventsCtr = ctr

