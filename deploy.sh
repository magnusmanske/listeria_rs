#!/usr/bin/env bash
# Delopy and restart toolforge services
ssh login.toolforge.org 'become listeria bash -lc "cd ~/listeria_rs/ && git pull && ./build.sh && ./restart.sh && webservice restart && echo \"OK\""'
