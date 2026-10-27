#!/usr/bin/env bash
# Builds all modules (with tests unless --skip-tests) and the container images used by Helm.
# OrbStack's Kubernetes uses the local Docker engine, so images are available without a registry.
source "$(dirname "$0")/lib/common.sh"

MVN_ARGS=(-B -q install)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-tests) MVN_ARGS+=(-DskipTests); shift ;;
    *) die "unknown option $1 (supported: --skip-tests)" ;;
  esac
done

export JAVA_HOME="$(java21_home)"
log "maven build (${MVN_ARGS[*]})"
(cd "$ROOT" && mvn "${MVN_ARGS[@]}")

for module in "${MODULES_WITH_IMAGES[@]}"; do
  log "docker image aggbench/$module:$IMAGE_TAG"
  docker build -q -f "$ROOT/docker/Dockerfile.jvm" --build-arg MODULE="$module" \
    -t "aggbench/$module:$IMAGE_TAG" "$ROOT" >/dev/null
done
ls -1 "$ROOT"/correctness-verifier/target/correctness-verifier.jar >/dev/null
log "build complete"
