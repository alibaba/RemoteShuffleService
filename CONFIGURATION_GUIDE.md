# Configuration Guide
This documentation contains RSS configuration details and a tuning guide.

## Important Configurations

### Environment Variables
- RSS_WORKER_MEMORY=4g
- RSS_WORKER_OFFHEAP_MEMORY=24g

RSS workers tend to improve performance by using off-heap buffers.
Off-heap memory requirement can be estimated as below:

```math
numDirs = `rss.worker.base.dirs`   the amount of directory will be used by RSS storage
bufferSize = `rss.worker.flush.buffer.size`   the amount of memory will be used by a single flush buffer 
off-heap-memory = bufferSize * estimatedTasks * 2 + network memory
```

For example, if an RSS worker has 10 storage directories or disks and the buffer size is set to 256 kilobytes.
The necessary off-heap memory is 10 gigabytes.
NetWork memory will be consumed when netty reads from a TPC channel, there will need some extra
memory. Empirically, RSS worker off-heap memory should be set to `(numDirs  * bufferSize * 1.2)`.

### Client-Side Configurations

|                  Item                   | Default |                                                                                                                                          Description                                                                                                                                          |
|:---------------------------------------:|:-------:|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|        spark.rss.master.address         |         |                                                                                                                Single master mode: address(host:port) of RSS Master,necessary                                                                                                                 |
|        spark.rss.ha.master.hosts        |         |                                                                                                                                 Ha mode: hosts of RSS Master                                                                                                                                  |
|          spark.rss.master.host          |         |                                                                                                                               Single master: host of RSS Master                                                                                                                               |
|          spark.rss.master.port          |         |                                                                                                                                      Port of RSS Master                                                                                                                                       |
|     spark.rss.push.data.buffer.size     |   64k   | Amount of reducer partition buffer memory. Buffered data will be sent to RSS worker if buffer is full. For performance consideration keep this buffer size higher than 32K. Example: If reducer amount is 2000,buffer size is 64K and task will consume up to 64K * 2000 = 125 M heap memory. |
|   spark.rss.push.data.queue.capacity    |   512   |                                                                    Push buffer queue size for a task. The maximum memory is `spark.rss.push.data.buffer.size` * `spark.rss.push.data.queue.capacity`(64K * 512 = 32M)                                                                         |
|   spark.rss.push.data.maxReqsInFlight   |   32    |                                                      Amount of netty in-flight requests. The maximum memory is `rss.push.data.maxReqsInFlight` * `spark.rss.push.data.buffer.size` * compression ratio(1 in worst case)(64K * 32 = 2M )                                                       |
|    spark.rss.limit.inflight.timeout     |  240s   |                                                                                                                       Timeout for netty in-flight requests to be done.                                                                                                                        |
|      spark.rss.fetch.chunk.timeout      |  120s   |                                                                                                                              Timeout for a task to fetch chunk.                                                                                                                               |
|  spark.rss.fetch.chunk.maxReqsInFlight  |    3    |                                                                                                                           Amount of in-flight chunk fetch request.                                                                                                                            |
|        spark.rss.data.io.threads        |    8    |                                                                                                                         Amount of thread count for task to push data.                                                                                                                         |
|      spark.rss.push.data.replicate      |  true   |                                                                            When true the RSS worker will replicate shuffle data to another RSS worker to ensure shuffle data won't be lost after the node failure.                                                                            |
| spark.rss.application.heartbeatInterval |   10s   |                                                                                                                                Application heartbeat interval.                                                                                                                                |
|       spark.rss.stage.end.timeout       |  240s   |                                                                                                                                    Time out for StageEnd.                                                                                                                                     |
|      spark.rss.shuffle.writer.mode      |  hash   |                                 RSS support two different shuffle writers. Hash-based shuffle writer works fine when shuffle partition count is normal. Sort-based shuffle writer works fine when memory pressure is high or shuffle partition count it huge.                                 |

### RSS Master Configurations

|                     Item                     |         Default         |                                          Description                                           |
|:--------------------------------------------:|:-----------------------:|:----------------------------------------------------------------------------------------------:|
|              rss.worker.timeout              |          120s           |                                                                                                |
|           rss.application.timeout            |          120s           |                                                                                                |
|           rss.rpc.io.clientThreads           | min{64, availableCores} |                                                                                                |
|           rss.rpc.io.serverThreads           | min{64, availableCores} |                                                                                                |
|           rss.master.port.maxretry           |            1            |               When RSS master port is occupied,we will retry for maxretry times.               |
|       rss.rpc.io.numConnectionsPerPeer       |            1            |              Connections between hosts are reused in order to reduce connection.               |
|                rss.ha.enabled                |          false          |   When true, RSS will activate raft implementation and sync shared data on master clusters.    |
|             rss.ha.master.hosts              |                         |                                   Master hosts address list.                                   |
|              rss.ha.service.id               |                         |                 When this config is empty, RSS master will refuse to startup.                  |
|           rss.ha.nodes.{serviceId}           |                         |              Nodes list that deploy RSS master. ServiceId is `rss.ha.service.id`               |
|      rss.ha.address.{serviceId}.{node}       |     localhost:9872      |   RSS master's rpc address for raft implementation. Port can be ignored and defaults to 9872   |
|                 rss.ha.port                  |          9872           |                                 Rpc port between multi master                                  |
|              rss.ha.storage.dir              |       /tmp/ratis        |                        Directory of RSS master to store ratis metadata.                        |
|  rss.ha.ratis.snapshot.auto.trigger.enabled  |          true           |                       Weather to enable raft implementation's snapshot.                        |
| rss.ha.ratis.snapshot.auto.trigger.threshold |         200000          |                                                                                                |
|      rss.offer.slots.algorithm.version       |           V2            | There is two algorithms V1 and V2. V2 is recommended unless you encountered some malfunctions. |

### RSS Worker Configurations

|             Item                |          Default           |                                                                                                                                                                                                    Description                                                                                                                                                                                                    |
|:-------------------------------:|:--------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|
|      rss.worker.base.dirs       |                            | Directory list to store shuffle data. Storage size limit can be set for each directory. For the sake of performance, there should be no more than 2 directories on the same disk partition if you are using HDD. There can be 4 or more directories can run on the same disk partition if you are using SSD. For example: dir1[:capacity=][:disktype=][:flushthread=],dir2[:capacity=][:disktype=][:flushthread=] |
|  rss.worker.flush.buffer.size   |            256K            |                                                                                                                                                                                                                                                                                                                                                                                                                   |
| rss.worker.flush.queue.capacity |            512             |                                                                    Size of buffer queue attached to each storage directory. Each flush buffer queue consumes `rss.worker.flush.buffer.size` * `rss.worker.flush.queue.capacity`(256K * 512 = 128M) off-heap memory. This config can be used to estimate RSS worker's off-heap memory demands.                                                                     |
|   rss.worker.fetch.chunk.size   |             8m             |                                                                                                                            Max chunk size of reducer's merged shuffle data. For example, if a reducer's shuffle data is 128 M and the data will need 16 fetch chunk requests to fetch.                                                                                                                            |
|       rss.push.io.threads       | `rss.worker.base.dirs` * 2 |                                                                                                                                                                                                                                                                                                                                                                                                                   |
|      rss.fetch.io.threads       | `rss.worker.base.dirs` * 2 |                                                                                                                                                                                                                                                                                                                                                                                                                   |
|       rss.master.address        |                            |                                                                                                                                                                          Single master mode: address(host:port) of RSS Master,necessary                                                                                                                                                                           |
|         rss.master.host         |                            |                                                                                                                                                                                         Single master: host of RSS Master                                                                                                                                                                                         |
|       rss.ha.master.hosts       |                            |                                                                                                                                                                                           Ha mode: hosts of RSS Master                                                                                                                                                                                            |
|         rss.master.port         |                            |                                                                                                                                                                                                Port of RSS Master                                                                                                                                                                                                 |

### Metrics

|              Item                 | Default   | Description  |
|:---------------------------------:|:---------:|:------------:|
|    rss.metrics.system.enabled     |   true    |              |
| rss.master.prometheus.metric.port |   9098    |              |
| rss.worker.prometheus.metric.port |   9096    |              |

#### metrics.properties

```properties
*.sink.csv.class=com.aliyun.emr.rss.common.metrics.sink.CsvSink
*.sink.prometheusServlet.class=com.aliyun.emr.rss.common.metrics.sink.PrometheusServlet
```

## Tuning

Assume we have a cluster described as below:
5 RSS Workers with 20 GB off-heap memory and 10 disks.
As we need to reserve 20% off-heap memory for netty, so we could assume 16 GB off-heap memory can be used for flush buffers.

If `spark.rss.push.data.buffer.size` is 64 KB, we can have in-flight requests up to 1310720.
If you have 8192 mapper tasks , you could set `spark.rss.push.data.maxReqsInFlight=160` to gain performance improvements.

If `rss.worker.flush.buffer` is 256 KB, we can have total slots up to 327680 slots.
So we should set `rss.worker.flush.queue.capacity=6553` and each RSS worker has 65530 slots.

## APPENDIX RSS Configuration List

### Environment Variables(rss-env.sh)

|            Item             |                Default                | Description |
|:---------------------------:|:-------------------------------------:|:-----------:|
|         `RSS_HOME`          |  ``$(cd "`dirname "$0"`"/..; pwd)``   |             |
|       `RSS_CONF_DIR`        | `${RSS_CONF_DIR:-"${RSS_HOME}/conf"}` |             |
|     `RSS_MASTER_MEMORY`     |                 1 GB                  |             |
|     `RSS_WORKER_MEMORY`     |                 1 GB                  |             |
| `RSS_WORKER_OFFHEAP_MEMORY` |                 1 GB                  |             |
|   `RSS_MASTER_JAVA_OPTS`    |                                       |             |
|   `RSS_WORKER_JAVA_OPTS`    |                                       |             |
|        `RSS_PID_DIR`        |          `${RSS_HOME}/pids`           |             |
|        `RSS_LOG_DIR`        |          `${RSS_HOME}/logs`           |             |
|       `RSS_SSH_OPTS`        |     `-o StrictHostKeyChecking=no`     |             |
|         `RSS_SLEEP`         |                                       |             |

### DefaultConfigs(rss-defaults.conf)

|                                Item                                 |               Default                |  Type   |                                                            Description                                                            |
|:-------------------------------------------------------------------:|:------------------------------------:|:-------:|:---------------------------------------------------------------------------------------------------------------------------------:|
|                     `rss.push.data.buffer.size`                     |                64 KiB                | String  |                                                                                                                                   |
|                   `rss.push.data.queue.capacity`                    |                 512                  |   int   |                                                                                                                                   |
|                   `rss.push.data.maxReqsInFlight`                   |                  32                  |   int   |                                                                                                                                   |
|                      `rss.fetch.chunk.timeout`                      |                120 s                 | String  |                                                                                                                                   |
|                  `rss.fetch.chunk.maxReqsInFlight`                  |                  3                   |   int   |                                                                                                                                   |
|                      `rss.push.data.replicate`                      |                 true                 |  bool   |                                                                                                                                   |
|                        `rss.worker.timeout`                         |                120 s                 | String  |                                                                                                                                   |
|                      `rss.application.timeout`                      |                120 s                 | String  |                                                                                                                                   |
|                     `rss.remove.shuffle.delay`                      |                 60 s                 | String  |                                                                                                                                   |
|                      `rss.get.blacklist.delay`                      |                 30s                  | String  |                                                                                                                                   |
|                        `rss.master.address`                         | `Utils.localHostName() + ":" + 9097` | String  |                                                                                                                                   |
|                          `rss.master.host`                          |  `rss.master.address` hostname part  | String  |                                                                                                                                   |
|                          `rss.master.port`                          |    `rss.master.address` port part    |   int   |                                                                                                                                   |
|                  `rss.worker.replicate.numThreads`                  |                  64                  |   int   |                                                                                                                                   |
|              `rss.worker.asyncCommitFiles.numThreads`               |                  32                  |   int   |                                                                                                                                   |
|                   `rss.worker.flush.buffer.size`                    |               256 KiB                | String  |                                                                                                                                   |
|                  `rss.worker.flush.queue.capacity`                  |                 512                  |   int   |                                                                                                                                   |
|                    `rss.worker.fetch.chunk.size`                    |                8 MiB                 | String  |                                                                                                                                   |
|                      `rss.rpc.max.parallelism`                      |                 1024                 |   int   |                                                                                                                                   |
|                  `rss.register.shuffle.max.retry`                   |                  3                   |   int   |                                                                                                                                   |
|                  `rss.register.shuffle.retry.wait`                  |                  3s                  |   int   |                                                                                                                                   |
|                   `rss.reserve.slots.max.retry`                     |                  3                   |   int   |                                                                                                                                   |
|                   `rss.reserve.slots.retry.wait`                    |                  3s                  | String  |                                                                                                                                   |
|                         `rss.flush.timeout`                         |                240 s                 | String  |                                                                                                                                   |
|                  `rss.expire.nonEmptyDir.duration`                  |                 3 d                  | String  |                                                                                                                                   |
|             `rss.expire.nonEmptyDir.cleanUp.threshold`              |                  10                  |   int   |                                                                                                                                   |
|                   `rss.expire.emptyDir.duration`                    |                 2 h                  | String  |                                                                                                                                   |
|                       `rss.worker.base.dirs`                        |                                      | String  |                                                                                                                                   |
|                    `rss.worker.base.dir.prefix`                     |             `/mnt/disk`              | String  |                                                                                                                                   |
|                    `rss.worker.base.dir.number`                     |                  16                  |   int   |                                                                                                                                   |
|                `rss.worker.unavailable.dirs.remove`                 |                 true                 |  bool   |                                                                                                                                   |
|                       `rss.stage.end.timeout`                       |                120 s                 | String  |                                                                                                                                   |
|                    `rss.limit.inflight.timeout`                     |                240 s                 | String  |                                                                                                                                   |
|                  `rss.limit.inflight.sleep.delta`                   |                50 ms                 | String  |                                                                                                                                   |
|                        `rss.pushserver.port`                        |                  0                   |   int   |                                                                                                                                   |
|                       `rss.fetchserver.port`                        |                  0                   |   int   |                                                                                                                                   |
|                    `rss.register.worker.timeout`                    |                180 s                 | String  |                                                                                                                                   |
|                     `rss.master.port.maxretry`                      |                  1                   |   int   |                                                                                                                                   |
|                   `rss.pushdata.retry.thread.num`                   |                  8                   |   int   |                                                                                                                                   |
|                    `rss.metrics.system.enabled`                     |                 true                 |  bool   |                                                                                                                                   |
|               `rss.metrics.system.timer.sliding.size`               |                 4000                 |   int   |                                                                                                                                   |
|                  `rss.metrics.system.sample.rate`                   |                  1                   | double  |                                                                                                                                   |
|              `rss.metrics.system.sliding.window.size`               |                 4096                 |   int   |                                                                                                                                   |
|                      `rss.inner.metrics.size`                       |                 4096                 |   int   |                                                                                                                                   |
|                 `rss.master.prometheus.metric.port`                 |                 9098                 |   int   |                                                                                                                                   |
|                 `rss.worker.prometheus.metric.port`                 |                 9096                 |   int   |                                                                                                                                   |
|                   `rss.merge.push.data.threshold`                   |                1 MiB                 | String  |                                                                                                                                   |
|                    `rss.driver.metaService.port`                    |                  0                   |   int   |                                                                                                                                   |
|                  `rss.worker.closeIdleConnections`                  |                false                 |  bool   |                                                                                                                                   |
|                          `rss.ha.enabled`                           |                false                 |  bool   |                                                                                                                                   |
|                        `rss.ha.master.hosts`                        |         `rss.master.host` 的值         | String  |                                                                                                                                   |
|                         `rss.ha.service.id`                         |                                      | String  |                                                                                                                                   |
|                     `rss.ha.nodes.<serviceId>`                      |                                      | String  |                                                                                                                                   |
|                `rss.ha.address.<serviceId>.<nodeId>`                |                                      | String  |                                                                                                                                   |
|                            `rss.ha.port`                            |                 9872                 |   int   |                                                                                                                                   |
|                        `rss.ha.storage.dir`                         |             `/tmp/ratis`             | String  |                                                                                                                                   |
|                    `rss.device.monitor.enabled`                     |                 true                 | boolean |                                                 Whether to enable device monitor                                                  |
|                      `rss.disk.check.interval`                      |                 15s                  | String  |                                            How frequency DeviceMonitor checks IO hang                                             |
|                      `rss.slow.flush.interval`                      |                 10s                  | String  |                                               Threshold that determines slow flush                                                |
|                         `rss.sys.block.dir`                         |             "/sys/block"             | String  |                                            Directory to read device stat and inflight                                             |
|                `rss.create.file.writer.retry.count`                 |                  3                   |   Int   |                                               Worker create FileWriter retry count                                                |
|                `rss.disk.space.safe.watermark.size`                 |                 0GB                  | String  |                                        Disk usage watermark size in GB, size must be Long                                         |
|                  `rss.worker.status.check.timeout`                  |                 10s                  | String  |                                                    Worker device check timeout                                                    |
|             `rss.worker.offheap.memory.critical.ratio`              |                 0.9                  |  float  |                                          Worker direct memory usage critical level ratio                                          |
|                 `rss.worker.memory.check.interval`                  |                  10                  |   int   |                                                      Timeunit is millisecond                                                      |
|                 `rss.worker.memory.report.interval`                 |                 10s                  | String  |                                                        Timeunit is second                                                         |
|                   `rss.partition.split.threshold`                   |                 256m                 | String  |                                                      Shuffle file split size                                                      |
|                     `rss.partition.split.mode`                      |                 soft                 | String  | sort, the shuffle file size might be larger than split threshold ; hard, the shuffle file size will be limited to split threshold |
|                    `rss.client.split.pool.size`                     |                  8                   |   int   |                                 Thread number to process shuffle split request in shuffle client.                                 |
|                    `rss.partition.sort.timeout`                     |                 220                  |   int   |                                                Timeout for a shuffle file to sort                                                 |
|                 `rss.memory.trim.action.threshold`                  |                  10                  |   int   |                                                                                                                                   |
|                `rss.partition.sort.memory.max.ratio`                |                 0.1                  | double  |                                                     Max ratio of sort memory                                                      |
|                  `rss.pause.pushdata.memory.ratio`                  |                 0.85                 | double  |                                                   If direct memory usage reach                                                    |
|         this limit, worker will stop receive from executor          |                                      |         |                                                                                                                                   |
|                 `rss.pause.replicate.memory.ratio`                  |                 0.95                 | double  |                                                   If direct memory usage reach                                                    |
| this limit, worker will stop receive from executor and other worker |                                      |         |                                                                                                                                   |
|                      `rss.resume.memory.ratio`                      |                 0.5                  | double  |                                             If direct memory usage is less than this                                              |
|                  limit, worker will resume receive                  |                                      |         |                                                                                                                                   |
|              `rss.worker.reserveForSingleSort.memory`               |                 1mb                  | string  |                                       Reserve memory when sorting a shuffle file off-heap.                                        |
|                         `rss.storage.hint`                          |                memory                | string  |                                         Available enumerations : memory,ssd,hdd,hdfs,oss                                          |
|                        `rss.partition.size`                         |                 int                  | string  |                     Estimated partition size, default is 64m, and it will change according to runtime stats.                      |
|                  `rss.disk.flusher.useMountPoint`                   |                 bool                 |  true   |     True means that each disk will get one disk flush. False means that a disk flush will be attached to a working directory.     |
|                        `rss.rpc.askTimeout`                         |                 240s                 | string  |                                                 Timeout for sending rpc messages.                                                 |
|                       `rss.rpc.lookupTimeout`                       |                 30s                  | string  |     Timeout for creating new connection. This value should be less than `rss.worker.timeout` to avoid worker lost in HA mode.     |
|                    `rss.haclient.rpc.askTimeout`                    |                 30s                  | string  | Timeout for HA client sending rpc messages. This value should be less than `rss.worker.timeout` to avoid worker lost in HA mode.  |
