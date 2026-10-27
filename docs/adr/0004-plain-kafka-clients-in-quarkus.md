# ADR 0004 – Plain Kafka clients inside Quarkus

**Status:** accepted

**Context.** Quarkus' Kafka/Kafka Streams extensions manage client versions and configuration implicitly; the
benchmark must pin Kafka 4.2.2 and make every setting reviewable.

**Decision.** Quarkus provides CDI, HTTP (health, metrics, status) and configuration; `KafkaStreams`,
`KafkaConsumer` and `KafkaProducer` are created explicitly from `Properties` built in one class per variant.

**Consequences.** Kafka 4.2.2 is used regardless of the Quarkus BOM (which manages 4.2.1 in 3.40.1); JVM mode only
(no native image), as required.
