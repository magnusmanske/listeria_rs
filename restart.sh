#!/bin/bash
toolforge jobs delete rustbot
toolforge jobs run --mem 5000Mi --cpu 3 --continuous --mount=all \
--image tool-listeria/tool-listeria:latest \
--command "sh -c 'target/release/main wikidata --config /data/project/listeria/listeria_rs/config.json'" \
rustbot
