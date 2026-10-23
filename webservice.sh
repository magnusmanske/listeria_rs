#!/bin/bash
# (Re)starts the website from the image ./build.sh built: the `web` entry of
# the Procfile. Stopping first also replaces the old PHP web service.
toolforge webservice stop
toolforge webservice buildservice start --mount=all
