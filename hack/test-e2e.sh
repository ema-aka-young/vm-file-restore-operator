#!/bin/bash
set -ex

# Silence "Entering/Leaving directory" when this script is invoked via `make cluster-functest`.
export MAKEFLAGS="${MAKEFLAGS:+$MAKEFLAGS }--no-print-directory"

export KUBEVIRT_NUM_NODES="${KUBEVIRT_NUM_NODES:-2}"
export KUBEVIRT_STORAGE="${KUBEVIRT_STORAGE:-rook-ceph-default}"

make cluster-down
make cluster-up

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.sh
source "${script_dir}/config.sh"
export KUBECONFIG="$("${REPO_ROOT}/kubevirtci/cluster-up/kubeconfig.sh")"

# Resolve localhost:<port> push URL and in-cluster pull URL from kubevirtci (not always :5000).
# shellcheck source=kubevirtci-image-env.sh
source "${script_dir}/kubevirtci-image-env.sh"

make cluster-sync
make test-e2e
