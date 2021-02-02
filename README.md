# Spark Shuffle Service with Cache Capability

A per-node long-running service that serve spark shuffle data read.

This service is meant to be spark version agnostic: both spark 2 and spark 3 shuffle read could be satisfied using this single service with the monolithic cache.

## Usage

Start the service:

`sbin/start-shuffle-service.sh`

Stop the service:

`sbin/stop-shuffle-service.sh`

Show status of the service:

`sbin/ss-daemon.sh status`
