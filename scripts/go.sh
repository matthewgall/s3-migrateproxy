#!/usr/bin/env bash
# Wrapper that uses the matching Go toolchain in the module cache if the
# system's go wrapper is an older bootstrap (e.g. go1.25.1 auto-downloading go1.26.0).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root_dir="$(cd "${script_dir}/.." && pwd)"

version=$(grep '^go ' "${root_dir}/go.mod" | cut -d' ' -f2)
candidate="${HOME}/go/pkg/mod/golang.org/toolchain@v0.0.1-go${version}.linux-amd64/bin/go"

if [ -x "${candidate}" ]; then
	export GOTOOLCHAIN=local
	export GOROOT="$(cd "$(dirname "${candidate}")/.." && pwd)"
	# Keep a separate build cache so a GOTOOLCHAIN wrapper's older artifacts
	# do not collide with the real toolchain.
	export GOCACHE="${HOME}/.cache/go-${version}"
	mkdir -p "${GOCACHE}"
	exec "${candidate}" "$@"
else
	exec go "$@"
fi
