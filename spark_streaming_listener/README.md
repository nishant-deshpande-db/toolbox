## A simple spark streaming listener that writes to a local filesystem.

Spark streaming listeners allow monitoring of spark streaming jobs, including metrics like the amount of data read from the source, written to the sink, time taken, and numerous other stats.

Streaming listeners are often designed to push events to a low latency write message bus like Kafka.

This can be inconvenient if you don't have (easy) access to a message bus, especially when testing.

The streaming listener here writes to a local filesystem. There are options to write every N events, so the write does not take up too many resources. Obviously N > 1 and the listener dies for some reason, there can be data loss.

If your requirement is for no possible data loss, push each event to a message bus. Otherwise this is an easy alternative.

The Databricks notebook "streaming listener usage example" has an example that should work in general, but has some specific nicities around using dbfs or Volumes in the Databricks environment.

Note that a single listener will listen to multiple spark streams on the same spark driver/cluster. So name each stream for an easy way to distinguish between events from different streams. The example shows this.


