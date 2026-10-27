#!/usr/bin/env bash
# Builds all modules (with tests unless --skip-tests) and the container images used by Helm.
# OrbStack's Kubernetes uses the local Docker engine, so images are available without a registry.
source "$(dirname "$0")/lib/common.sh"

#   --native   also builds the aggregators as native executables (Oracle GraalVM Native Image, G1 GC) into images
#              tagged <tag>-native; run them with ./scripts/benchmark.sh --build native
MVN_ARGS=(-B -q install)
NATIVE=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-tests) MVN_ARGS+=(-DskipTests); shift ;;
    --native) NATIVE=true; shift ;;
    *) die "unknown option $1 (supported: --skip-tests, --native)" ;;
  esac
done

export JAVA_HOME="$(java25_home)"
log "maven build (${MVN_ARGS[*]})"
(cd "$ROOT" && mvn "${MVN_ARGS[@]}")

for module in "${MODULES_WITH_IMAGES[@]}"; do
  log "docker image aggbench/$module:$IMAGE_TAG"
  docker build -q -f "$ROOT/docker/Dockerfile.jvm" --build-arg MODULE="$module" \
    -t "aggbench/$module:$IMAGE_TAG" "$ROOT" >/dev/null
done
if [[ "$NATIVE" == true ]]; then
  # native-image runs inside the Oracle GraalVM container, so the executables are Linux binaries for the
  # Docker engine's architecture. G1 is available only in Oracle GraalVM (not GraalVM CE or Mandrel).
  for module in "${NATIVE_MODULES[@]}"; do
    log "native executable for $module ($NATIVE_BUILDER_IMAGE, G1)"
    (cd "$ROOT" && mvn -B -q package -pl "$module" -DskipTests \
      -Dquarkus.native.enabled=true \
      -Dquarkus.native.container-build=true \
      -Dquarkus.native.builder-image="$NATIVE_BUILDER_IMAGE" \
      -Dquarkus.native.container-runtime-options=--workdir=/project \
      -Dquarkus.native.additional-build-args=--gc=G1 \
      -Dquarkus.native.native-image-xmx="${AGGBENCH_NATIVE_BUILD_XMX:-6g}")
    log "docker image aggbench/$module:$IMAGE_TAG-native"
    docker build -q -f "$ROOT/docker/Dockerfile.native" --build-arg MODULE="$module" \
      -t "aggbench/$module:$IMAGE_TAG-native" "$ROOT" >/dev/null
  done
fi
ls -1 "$ROOT"/correctness-verifier/target/correctness-verifier.jar >/dev/null
log "build complete"
