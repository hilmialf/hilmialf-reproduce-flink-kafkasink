KafkaSink generates new transactional Id per checkpoint.
This can be problematic as it consumes broker heap and may even cause broker OOM.
This is just a simple program to reproduce the behavior.

Just launch your kubernetes, then:
1. Launch your kafka server
2. Launch the KafkaSinkMimic
