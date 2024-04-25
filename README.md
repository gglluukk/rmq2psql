# rmq2psql: RabbitMQ to PostgreSQL Data Pipeline

`rmq2psql` investigates various approaches for a data pipeline that retrieves messages from RabbitMQ and stores them in PostgreSQL using Golang and Python, including benchmarks.



## Performance Analysis

Each [benchmark](benchmark.sh) conducts 10,000 reads from RabbitMQ and executes bulk inserts of 100 records into PostgreSQL, with results shown per second.


### Goland vs Python

```
rmq2psql    (Golang)                        | 2.480
rmq2psql.py (Python, best of)               | 3.612

```
* with [golang/rmq2psql/rmq2psql.go](golang/rmq2psql/rmq2psql.go) and [rmq2psql.py](rmq2psql.py)


### Python JSON libraries
```
rmq2psql.py (Python, msgspec)               | 3.638
rmq2psql.py (Python, msgspec_struct)        | 3.642
rmq2psql.py (Python, orjson)                | 3.612
rmq2psql.py (Python, ujson)                 | 3.741
```
* see [logs/benchmark__json.log](logs/benchmark__json.log)


### Python RabbitMQ Types Consuming
```
rmq2psql.py (Python, message processing)    | 3.717
rmq2psql.py (Python, queue without timeout) | 4.564
rmq2psql.py (Python, queue with timeout)    | 4.742
```

* see [logs/benchmark__loop.log](logs/benchmark__loop.log)
* with [aio_pika patch](aio_pika-9.4.1.diff) applied or `import aio_pika_patch` via [aio_pika_patch.py](aio_pika_patch.py), otherwise don't expect any differences for queue with or without timeout


### Profiling

For per-line `rmq2psql.py` profiling of JSON libraries and RabbitMQ Types Consuming, the following files are provided in [logs/](logs/)

* [profile__message_processing.log](logs/profile__message_processing.log)
* [profile__queue_iteration_without_timeout.log](logs/profile__queue_iteration_without_timeout.log)
* [profile__queue_iteration_with_timeout.log](logs/profile__queue_iteration_with_timeout.log)


### Recommendations

- **JSON Library**:
  - needs to be investigated further with actual JSON data from the project

- **RabbitMQ Types Consuming**:
  - use `message_processing` for optimal performance:
    ```
            async for message in queue:
                async with message.process(ignore_processed, ...):
                    await self.rmq_callback(message=message)

    ```
