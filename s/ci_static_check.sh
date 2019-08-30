#!/bin/bash
set -xe

wget -q -O staticcheck.tar.gz https://github.com/dominikh/go-tools/releases/download/latest/staticcheck_linux_amd64.tar.gz
tar --strip-components 1 -C . -xf staticcheck.tar.gz staticcheck/staticcheck
chmod ug+x ./staticcheck
./staticcheck ./...
