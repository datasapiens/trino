#!/bin/bash

docker run -it \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v $M2_PATH/.m2:/root/.m2 \
    -v $TRINO_REPO_PATH:/trino \
    --workdir /trino \
    trino_pinot_tests:latest bash -c "\
      ./mvnw -Dtest=TestPinotIntegrationSmokeTest test -pl :trino-pinot \
    "

# ./mvnw -Dtest=TestDynamicTable test -pl :trino-pinot \