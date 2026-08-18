<div align="center">

<img src="docs/ckn-logo.png" alt="CKN Logo" width="200"/>

# Cyberinfrastructure Knowledge Network

[![Documentation Status](https://img.shields.io/badge/docs-latest-blue.svg)](https://cyberinfrastructure-knowledge-network.readthedocs.io/en/latest/)
[![Build Status](https://github.com/Plale-Lab/cyberinfrastructure-knowledge-network/actions/workflows/ci.yml/badge.svg)](https://github.com/Plale-Lab/cyberinfrastructure-knowledge-network/actions)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

</div>

The **Cyberinfrastructure Knowledge Network (CKN)** is an extensible and portable distributed framework designed to optimize AI at the edge—particularly in dynamic environments where workloads may change suddenly (for example, in response to motion detection). CKN enhances edge–cloud collaboration by using historical data, graph representations, and adaptable deployment of AI models to satisfy changing accuracy‑and‑latency demands on edge devices. It integrates with the [Patra Knowledge Base](https://github.com/Plale-Lab/patra-knowledge-base) to provide end-to-end model provenance from training through edge deployment.

**Tags:** CI4AI, PADI

For guidance on what Tutorials, How-To Guides, and Explanation content covers, see [Diátaxis](https://diataxis.fr/).

### License

The Cyberinfrastructure Knowledge Network (CKN) is copyrighted by Plale Lab at The University of Oregon and is distributed under the BSD 3-Clause License. See `LICENSE` for more information.

## References

- [Documentation](https://cyberinfrastructure-knowledge-network.readthedocs.io/en/latest/)
- [CKN paper](https://ieeexplore.ieee.org/document/10254827)
- [Patra Knowledge Base](https://github.com/Plale-Lab/patra-knowledge-base)
- [CKN topics reference](docs/topics.md)

## Acknowledgements

This work has been funded by grants from the National Science Foundation, and in part through Plale Lab at The University of Oregon.

*National Science Foundation (NSF) funded AI institute for Intelligent Cyberinfrastructure with Computational Learning in the Environment (ICICLE) (OAC 2112606)*

## Issue reporting

Report issues via [GitHub Issues](https://github.com/Plale-Lab/cyberinfrastructure-knowledge-network/issues).

---

# Tutorials

### Create a Custom CKN Plug-in

#### 1. Create a CKN Topic

We will create a CKN topic named `temperature-sensor-data` to store temperature events. The CKN topics and their details are mentioned [here](docs/topics.md).

Update `docker-compose.yml` (root directory) and add the topic to the broker environment:

```yaml
services:
  broker:
    environment:
      KAFKA_CREATE_TOPICS: "temperature-sensor-data:1:1"
```

Apply the change:

```bash
make down
make up
```


#### 2. Produce Events

Create a producer script `produce_temperature_events.py` and run it.

```python
from confluent_kafka import Producer
import json, time

producer = Producer({"bootstrap.servers": "localhost:9092"})

try:
    for i in range(10):
        for sensor_id in ["sensor_1", "sensor_2", "sensor_3"]:
            event = {
                "sensor_id": sensor_id,
                "temperature": round(20 + 10 * (0.5 - time.time() % 1), 2),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            }
            producer.produce("temperature-sensor-data", key=sensor_id, value=json.dumps(event))
        producer.flush()
        time.sleep(1)
    print("Produced 10 events successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
```

Open a shell inside the broker container and start the consumer. You should see JSON‑formatted temperature events.

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic temperature-sensor-data --from-beginning
```


#### 3. Connect to a Data Sink

Create the connector configuration `pgsink-temperature-connector.json` and place it in `ckn_broker/`,
following the existing `pgsink-*.json` configs (Postgres is the supported sink for new pipelines —
see `ckn_broker/setup_connector.sh`).

```json
{
  "name": "PgSinkConnectorTemperature",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "temperature-sensor-data",
    "connection.url": "jdbc:postgresql://postgres:5432/d2i",
    "connection.user": "d2i",
    "connection.password": "d2i",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "table.name.format": "temperature_sensor_data",
    "value.converter.schemas.enable": true,
    "delete.enabled": false,
    "auto.create": true,
    "auto.evolve": false,
    "errors.tolerance": "all",
    "errors.log.enable": true
  }
}
```

#### 4. Register the connector

Add the `curl` registration call to `ckn_broker/setup_connector.sh` alongside the other JDBC sinks:

```bash
curl -X POST -H "Content-Type: application/json" \
     --data @/app/pgsink-temperature-connector.json \
     http://localhost:8083/connectors
```

Restart CKN and run the `temperature‑event` producer again.

```bash
make down
make up

python produce_temperature_events.py
```

Query the `temperature_sensor_data` table in Postgres to view the streamed data.
You have successfully set up a temperature‑monitoring plugin with CKN!

---

# How-To Guides

See the full [documentation](https://cyberinfrastructure-knowledge-network.readthedocs.io/en/latest/) for detailed instructions on creating custom plug‑ins and streaming events to the knowledge graph.

### Hosted Patra endpoints (Tapis Pods)

CKN itself is self‑hosted — you run the broker, knowledge graph, and dashboard on your own
infrastructure via `make up`. The [Patra Knowledge Base](https://github.com/Plale-Lab/patra-knowledge-base)
it reports model provenance to is hosted as [Tapis Pods](https://tapis.readthedocs.io/en/latest/technical/pods.html)
in the ICICLE tenant:

| Service | URL |
| ------- | --- |
| Patra REST API (stable) | `https://patrabackend.pods.icicleai.tapis.io` |
| Patra REST API (dev) | `https://patrabackend-dev.pods.icicleai.tapis.io` |
| Patra UI | `https://patra.pods.icicleai.tapis.io` |
| Tapis tenant | `https://icicleai.tapis.io` |

Point `patra_agent` and any Patra‑integrated plugin at the stable REST API. Writes require a
Tapis token from `https://icicleai.tapis.io/v3/oauth2/tokens`; the stable and dev backends share
one database, so treat writes to either as production writes.

### Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose) installed and running.
- Open network access to the following ports:
  - `7474` (Neo4j Web UI)
  - `7687` (Neo4j Bolt)
  - `2181` (ZooKeeper)
  - `9092` (Kafka Broker)
  - `8083` (Kafka Connect)
  - `8502` (CKN dashboard)

### Quick‑Start

#### 1. Clone the repository and start services

```bash
git clone https://github.com/Plale-Lab/cyberinfrastructure-knowledge-network.git
make up
```

After setup completes, verify that all modules are running:

```bash
docker compose ps
```

#### 2. Stream an example camera‑trap event

```bash
docker compose -f examples/docker-compose.yml up -d --build
```

View the streamed data on the [CKN dashboard](http://localhost:8502/Camera_Traps) or open the [neo4j browser](http://localhost:7474/browser/) and log in with the credentials mentioned in the docker-compose file. Run `MATCH (n) RETURN n` to view the streamed data.

Shut down services using:
```bash
make down
docker compose -f examples/docker-compose.yml down
```

---

# Explanation

CKN facilitates seamless connectivity between edge devices and the cloud through event streaming, enabling real‑time data capture and processing. By leveraging event‑stream processing, it captures, aggregates, and stores historical system‑performance data in a knowledge graph that models application behaviour and guides model selection and deployment at the edge.

CKN comprises several core components:

- **CKN Daemon** – A lightweight service that resides on each edge server. It manages communication with edge devices, handles requests, captures performance data, and deploys AI models as needed. The daemon connects with the cloud‑based CKN system via a pub/sub system, capturing real‑time events from edge devices (model usage, resource consumption, prediction accuracy, latency, and more).
- **Event Streaming & Processing** – Stream‑processing techniques (for example, tumbling windows) aggregate events and generate real‑time alerts from edge‑device streams.
- **Knowledge Graph** – A Neo4j graph database that stores historical and provenance information about applications, models, and edge events. This comprehensive view of the system enables CKN to track model usage and analyse performance over time.
- **Patra Integration** – CKN feeds edge inference events and deployment metrics into the [Patra Knowledge Base](https://github.com/Plale-Lab/patra-knowledge-base), linking runtime observations back to model cards for full lifecycle transparency.

The primary objective of CKN is to provide a robust framework for optimising AI‑application deployment and resource allocation at the edge. Leveraging real‑time event streaming and knowledge graphs, CKN efficiently handles AI workloads, adapts to changing requirements, and supports scalable edge–cloud collaboration.

Refer to this paper for more information: [https://ieeexplore.ieee.org/document/10254827](https://ieeexplore.ieee.org/document/10254827).
