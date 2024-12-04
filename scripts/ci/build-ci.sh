#!/bin/bash
set -euo pipefail

qt_dir=${1}
build_target=${2:-build_tests}

OS=$(uname)

source "$(dirname "$BASH_SOURCE")/impl/code-inspector.sh"
code_inspect "${ROOTPATH:-.}"

mkdir -p build
pushd build

if [[ "${RELEASE:-false}" == "true" ]]; then
    BUILD_TYPE="RelWithDebInfo"
fi

if [[ ${ASAN_INT:-0} -eq 1 ]]; then
    SANITIZERS="-DNANO_ASAN_INT=ON"
elif [[ ${ASAN:-0} -eq 1 ]]; then
    SANITIZERS="-DNANO_ASAN=ON"
elif [[ ${TSAN:-0} -eq 1 ]]; then
    SANITIZERS="-DNANO_TSAN=ON"
elif [[ ${LCOV:-0} -eq 1 ]]; then
    SANITIZERS="-DCOVERAGE=ON"
fi

ulimit -S -n 8192

if [[ "$OS" == 'Linux' ]]; then
    if clang --version && [ ${LCOV:-0} == 0 ]; then
        BACKTRACE="-DNANO_STACKTRACE_BACKTRACE=ON \
        -DNANO_BACKTRACE_INCLUDE=</tmp/backtrace.h>"
    else
        BACKTRACE="-DNANO_STACKTRACE_BACKTRACE=ON"
    fi
else
    BACKTRACE=""
fi

cmake \
-G'Unix Makefiles' \
-DACTIVE_NETWORK=nano_dev_network \
-DNANO_TEST=ON \
-DNANO_GUI=ON \
-DPORTABLE=1 \
-DNANO_WARN_TO_ERR=ON \
-DCMAKE_BUILD_TYPE=${BUILD_TYPE:-Debug} \
-DQt5_DIR=${qt_dir} \
${BACKTRACE:-} \
${SANITIZERS:-} \
..

if [[ "$OS" == 'Linux' ]]; then
    if [[ ${LCOV:-0} == 1 ]]; then
        cmake --build ${PWD} --target generate_coverage -- -j2
    else
        cmake --build ${PWD} --target ${build_target} -- -j2
    fi
else
    sudo cmake --build ${PWD} --target ${build_target} -- -j2
fi

popd
pushd rust
~/.cargo/bin/cargo test --no-run
popd
