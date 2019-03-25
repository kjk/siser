#!/usr/bin/env pwsh

go test -coverprofile cp.out
go tool cover -html cp.out
remove-item -Force -ErrorAction Ignore cp.out
