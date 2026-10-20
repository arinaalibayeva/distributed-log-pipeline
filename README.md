# Distributed Log Aggregation and Alerting Pipeline

A small distributed log in the spirit of Kafka. Simulated services (login, search, image upload) send log lines to a cluster of broker nodes. The brokers split the lines into one partition per service and replicate each partition with a leader and followers. Consumers (alerting, search indexing, archiving) each read the partitions at their own pace.

Final project for IL18105 Distributed Systems, Minerva University, Fall 2026.

## Status

Work in progress. Demo due October 29, 2026.
