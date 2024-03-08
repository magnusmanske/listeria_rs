#!/bin/bash
toolforge jobs delete single

toolforge jobs run --wait --mem 2000Mi --cpu 1 --mount=all --image tool-listeria/tool-listeria:latest \
-o /data/project/listeria/listeria_rs/single.out -e /data/project/listeria/listeria_rs/single.err \
--command "sh -c 'target/release/main \"$1\" \"$2\" /data/project/listeria/listeria_rs/config.json'" single

toolforge jobs logs single
