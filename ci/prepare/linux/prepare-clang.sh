#!/bin/bash
set -euox pipefail

# Clang installer dependencies
DEBIAN_FRONTEND=noninteractive apt-get install -yqq lsb-release software-properties-common gnupg

CLANG_VERSION=16

# TODO: Verify integrity (at this time, the clang build is not used for any production artifacts)
curl -O https://apt.llvm.org/llvm.sh && chmod +x llvm.sh && ./llvm.sh $CLANG_VERSION

update-alternatives --install /usr/bin/cc cc /usr/bin/clang-$CLANG_VERSION 100
update-alternatives --install /usr/bin/c++ c++ /usr/bin/clang++-$CLANG_VERSION 100
update-alternatives --install /usr/bin/lldb lldb /usr/bin/lldb-$CLANG_VERSION 100
