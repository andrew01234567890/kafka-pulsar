# kafka-pulsar

A Kafka wire-protocol plugin for [Apache Pulsar](https://pulsar.apache.org). It is packaged as a
Pulsar `ProtocolHandler` NAR that lets native Apache Kafka clients (producer, consumer, admin) talk
to a Pulsar broker — in the spirit of StreamNative's KoP — targeting maximum client compatibility.

> **Status: early.** The current build is a walking skeleton: the broker loads the NAR, binds the
> Kafka listener, and answers `ApiVersions`. The wider protocol surface (Metadata, Produce, Fetch,
> consumer groups, admin, security, transactions) is built up phase by phase.

## Requirements

- JDK 21 (Temurin recommended)
- Docker, and — for the end-to-end tests — [KIND](https://kind.sigs.k8s.io), `kubectl`, and Helm 3+

The build uses a committed Maven wrapper (`./mvnw`), so a local Maven install is not required.

## Build and test

```bash
# Compile, run unit + integration tests, and package the NAR
./mvnw -B verify
```

The NAR is written to `kafka-protocol-handler/target/kafka-protocol-handler-*.nar`.

### Test tiers

| Tier | Where | Runs in |
|------|-------|---------|
| Unit | `kafka-common` | every build (`verify`) |
| Integration | `kafka-protocol-handler` (Netty `EmbeddedChannel` + real kafka-clients) | every build (`verify`) |
| End-to-end | `e2e-tests` (`@Tag("e2e")`, Pulsar on KIND + real kafka-clients) | `-Pe2e` only |

### Run the end-to-end suite locally

```bash
./mvnw -B -pl kafka-protocol-handler -am package -DskipTests
docker build -t kafka-pulsar-broker:e2e -f docker/Dockerfile.broker .
kind create cluster --name kop-e2e
kind load docker-image kafka-pulsar-broker:e2e --name kop-e2e
helm repo add apache https://pulsar.apache.org/charts && helm repo update
helm install pulsar-ci apache/pulsar -n pulsar --create-namespace \
  --values deploy/values-standalone-kafka.yaml --wait --timeout 12m
POD=$(kubectl -n pulsar get pod -l component=standalone -o jsonpath='{.items[0].metadata.name}')
kubectl -n pulsar port-forward "$POD" 9092:9092 &
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092 ./mvnw -B -pl e2e-tests -Pe2e test
```

## Module layout

- `kafka-common` — pure, Pulsar-free logic (topic-name mapping, offset math, protocol helpers).
- `kafka-protocol-handler` — the `ProtocolHandler` implementation and Netty pipeline; produces the NAR.
- `e2e-tests` — cluster-level tests driving a Pulsar broker on KIND with real Kafka clients.

## Contributing

- All changes land through pull requests; the default branch is protected.
- PR titles must be `[MAJOR.MINOR.PATCH] Descriptive Title` (e.g. `[1.4.0] Add Fetch API`) and PRs
  must have a non-empty description. A required check enforces this.
- The release build (push to `main`) derives the version from the merged PR title, tags `vX.Y.Z`,
  and publishes the versioned NAR. PR builds are version-neutral.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
