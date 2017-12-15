## Kafka Tasks
- [X] Kafka Consumer
- [X] Confirm if parallelism hint is the same as consuming from different partitions (checked messages' partition no. within Storm bolts - they're distributed)
- [X] Confirm if parallelism hint is the same as producing to different partitions (subscribed to output topic and checked messages' partition no. - they're distributed)
- [X] Starting offset configuration (UNCOMMITTED_LATEST)
- [X] Kafka Producer

## Parsers
- [ ] Bro http parser
- [ ] Bro dns parser
- [ ] yaf parser
- [X] snort parser
- [ ] nio-flow parser
- [ ] nio-dns parser
- [ ] nio-http parser

## Bolts
- [ ] Hbase connector
    - [X] Insert records into hbase externally
    - [X] Read the values from database
    - [ ] Update hbase database, from bolt
    - [ ] Record caching to reduce number of reads
    - [ ] Convert threat intel aggregator to work with new setup

- [ ] GeoIP bolt
- [ ] threatintel bolt
- [ ] housekeeping bolts (CRUD hbase table)

## General Tasks
- [ ] CSV manipulator class for inter-bolts communication
- [ ] FailSafe Mechanism: A channel to display error messages, kafka topic
- [ ] Config inferface: Flux or alternatives (Readup metron mgmt UI's way of loading parser configs)
- [ ] Config interface: Schema reader, data structure, etc.
