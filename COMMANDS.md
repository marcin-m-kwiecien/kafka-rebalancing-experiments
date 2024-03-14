### Show consumers for consumer group
```shell
watch -n 1 ./kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group latency-test-consumer-group
```