#!/usr/bin/env bash
# -*- mode: shell-script; indent-tabs-mode: nil; sh-basic-offset: 4; -*-
# ex: ts=8 sw=4 sts=4 et filetype=sh

set -euo pipefail

export MINIO_ROOT_USER='localadmin'
export MINIO_ROOT_PASSWORD='localdevsecret123'

if [ ! -d "$HOME/minio-data" ]; then
  mkdir -v $HOME/minio-data
fi

minio server "$HOME/minio-data" \
  --address 127.0.0.1:9000 \
  --console-address 127.0.0.1:9001
