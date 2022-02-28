#! /bin/bash

for i in {1..$1}
do
    docker run \
        -v "$HOME/config.yml:/config/config.yml" \
        -v "$HOME/messages.json:/messages/messages.json" \
        --name infinite-kafka-datasource-$(cat /dev/urandom | head -n 100 | tr -cd '[:alnum:]' | cut -c 1-49) \
        --net host -d  infinite-kafka-datasource:1.0.0
done