# ADR 0005 – Java 25, and native aggregator executables as a second build

**Status:** accepted (native build under evaluation; first results in ANALYSIS-NOTES note 6)

**Context.** In variant B, about 85–89 % of the events delayed by a rollout are delayed while a freshly started JVM
is still warming up (`docs/ANALYSIS-NOTES.md` note 5). A native executable is compiled ahead of time and has no
warm-up phase. Quarkus 3.36+ builds native executables only with GraalVM/Mandrel 25, which needs Java 25.
Oracle GraalVM is the only distribution with the G1 collector for native executables (GraalVM CE and Mandrel offer
only the stop-the-world Serial GC); it is free for production use under the GraalVM Free Terms and Conditions.
The original task fixed Java 21 and JVM mode; the change was requested explicitly.

**Decision.**
- All modules target Java 25; JVM images use Eclipse Temurin 25 (`docker/Dockerfile.jvm`).
- The two systems under test (`streams-aggregator`, `redis-aggregator`) can additionally be built as native
  executables with Oracle GraalVM 25 and `--gc=G1` (`scripts/build.sh --native`, `docker/Dockerfile.native`, images
  tagged `<tag>-native`) and run with `scripts/benchmark.sh --build native`. Downstream services, the load
  generator and the verifier stay on the JVM: they are the measurement environment, not the system under test.
- Native-image support for the Kafka client, Kafka Streams and RocksDB comes from the Quarkus extensions
  `quarkus-kafka-client` and `quarkus-kafka-streams`. The applications still create their clients and their
  `KafkaStreams` instance themselves (ADR 0004); the extensions start nothing.
- A native run and a JVM run are never mixed in one comparison: every run records `aggregatorBuild` and the image
  id in `environment.txt`, and each build's runs go to their own results directory (`AGGBENCH_RESULTS`).

**Consequences.**
- The 68-run campaign in `results/runs` was measured on Java 21; new comparisons need a Java 25 JVM baseline.
- Native executables trade warm-up for a lower peak speed than fully warmed C2 code (Oracle GraalVM narrows the
  gap with ML-inferred profiles by default; recorded profiles (PGO) are not used yet).
- JVM-specific metrics (JIT, some GC and memory pool series) are missing or differ in native runs.
- Oracle labels Native Image an Early Adopter technology in Oracle GraalVM 25, and later 25.x updates may move to a
  licence that requires a subscription for production use, as happened for Java 17 and 21.
