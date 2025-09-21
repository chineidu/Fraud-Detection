# Fraud-Detection
<!-- TOC -->

- [Fraud-Detection](#fraud-detection)
  - [Note](#note)

<!-- /TOC -->

## Note

- Consume the data from Kafka via CLI tool

```sh
kcat -b <host>:<port> -C -t <topic> -o end

# E.g.
kcat -b localhost:9092 -C -t weather_data_demo -o end
```
