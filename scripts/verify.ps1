#!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"

Write-Host "==> go test ./..."
go test ./...

Write-Host "==> go test ./test/..."
go test ./test/...

Write-Host "==> go test ./test/conformance/... -run TestConformanceSuite"
go test ./test/conformance/... -run TestConformanceSuite
