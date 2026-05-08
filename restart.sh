#!/bin/bash
toolforge jobs delete rustbot
toolforge jobs run --mem 5000Mi --cpu 3 --continuous --mount=all \
--image tool-listeria/tool-listeria:latest \
--command "target/release/main --config /data/project/listeria/listeria_rs/config.json wikidata" \
rustbot


# --command "sh -c 'target/release/main --config /data/project/listeria/listeria_rs/config.json wikidata'" \
