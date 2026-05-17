#!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"

Write-Host "==> install golangci-lint v2"
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

$lintBin = Join-Path (Join-Path (go env GOPATH) "bin") "golangci-lint"

Write-Host "==> golangci-lint version"
& $lintBin version

Write-Host "==> verify golangci-lint v2"
$versionOutput = & $lintBin version
if ($versionOutput -notmatch "version 2\.") {
	throw "golangci-lint v2 is required"
}

Write-Host "==> golangci-lint run --config .golangci.yml"
& $lintBin run --config .golangci.yml

Write-Host "==> go test ./..."
go test ./...

Write-Host "==> go test ./test/..."
go test ./test/...

Write-Host "==> go test ./test/conformance/... -run TestConformanceSuite"
go test ./test/conformance/... -run TestConformanceSuite
