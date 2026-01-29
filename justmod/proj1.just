set working-directory := '..'
set shell := ["bash","-cu"]
# list project 1 just recipes
default:
    @just --list p1 --unsorted

# install extra dependencies of your KV code
# The below recipe installs CMake, tools required to build gRPC, clones the gRPC modules, followed by the local build and install of gRPC and protobuf.
deps:
    export MY_INSTALL_DIR=$HOME/.local && \
    mkdir -p $MY_INSTALL_DIR && \
    export PATH="$MY_INSTALL_DIR/bin:$PATH" && \
    sudo apt install -y cmake && \
    cmake --version && \
    cd .. && \
    \
    sudo apt install -y build-essential autoconf libtool pkg-config && \
    \
    git clone --recurse-submodules -b v1.66.0 --depth 1 --shallow-submodules https://github.com/grpc/grpc && \
    \
    cd grpc && \
    mkdir -p cmake/build && \
    pushd cmake/build && \
    cmake -DgRPC_INSTALL=ON \
        -DgRPC_BUILD_TESTS=OFF \
        -DCMAKE_CXX_STANDARD=17 \
        -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
        ../.. && \
    make -j 4 && \
    make install && \
    popd

build:
    just p1::build-proto && \
    just p1::build-cmake

# build your executables in release mode
build-proto:
    # Step 1: Generate the C++ header and cc files using protoc
    cd ./impl && \
    mkdir -p generated && \
    protoc -I=protos --grpc_out=generated --cpp_out=generated --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ./protos/kvstore.proto

build-cmake:
    # From madkv directory
    export MY_INSTALL_DIR=$HOME/.local && \
    cd ./impl && \
    mkdir -p cmake/build && \
    pushd cmake/build && \
    cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../.. && \
    make -j 4

# clean the build of your executables
clean:
    rm -rf ./impl/cmake && \
    cd impl && \
    cd .. && \
    rm -rf ./impl/generated && \
    rm -rf ./ycsb

# run your KV store server
server listen="0.0.0.0:3777":
    ./impl/cmake/build/serverimpl {{listen}}

# run you KV store client in stdin/out interface mode
client server="127.0.0.1:3777" input="":
    if [ -n "{{input}}" ]; then \
        cat "{{input}}" | ./impl/cmake/build/clientimpl "{{server}}"; \
    else \
        ./impl/cmake/build/clientimpl "{{server}}"; \
    fi

# run your KV store testcase 1 client

# Recipe to check if a file exists
check_file_exists filepath:
    if [ -f "{{filepath}}" ]; then \
        exit 0; \
    else \
        echo "Error: File '{{filepath}}' not found!" >&2; \
        exit 1; \
    fi

# Recipe for test1, using check_file_exists
test1 server="127.0.0.1:3777" filepath="./tests/test1.txt":
    just p1::check_file_exists "{{filepath}}"
    just p1::client {{server}} "{{filepath}}"

# run your KV store testcase 2 client
test2 server="127.0.0.1:3777" filepath="./tests/test2.txt":
    just p1::check_file_exists "{{filepath}}"
    just p1::client {{server}} "{{filepath}}"

# run your KV store testcase 3 client
test3 server="127.0.0.1:3777":
    just p1::check_file_exists "./tests/test1.txt"
    just p1::check_file_exists "./tests/test2.txt"
    just p1::client {{server}} "./tests/test1.txt" & 
    just p1::client {{server}} "./tests/test2.txt" &
    wait  # Ensures both clients complete

# run your KV store testcase 4 client(s)
test4 server="127.0.0.1:3707":
    just p1::check_file_exists "./tests/test2_conflicting.txt"
    just p1::check_file_exists "./tests/test2.txt"
    just p1::client {{server}} "./tests/test2_conflicting.txt" & 
    just p1::client {{server}} "./tests/test2.txt" &
    wait  # Ensures both clients complete

# run your KV store testcase 5 client(s)
test5 server="127.0.0.1:3777":
    just p1::check_file_exists "./tests/test1_conflicting.txt"
    just p1::check_file_exists "./tests/test1.txt"
    just p1::client {{server}} "./tests/test1_conflicting.txt" & 
    just p1::client {{server}} "./tests/test1.txt" &
    just p1::client {{server}} "./tests/test1_conflicting.txt" & 
    just p1::client {{server}} "./tests/test1.txt" &
    wait  # Ensures both clients complete

# kill all processes of your KV store system
# TODO: Fix the return value issue.
kill:
    echo "Killing all clients, server, and helpers..."
    -pkill -f "./impl/cmake/build/clientimpl" >/dev/null 2>&1 ; \
    -pkill -f "./impl/cmake/build/serverimpl" >/dev/null 2>&1
    # FIXME: your kill commands here
    #        make sure that it kills all server, clients, and any extra helper
    #        processes of your system; redirecting both out & err to /dev/null
    #        is recommended
    #   TIP: commands such as 'pkill' may return non-zero exit code on success,
    #        which would by default abort the 'just' recipe early; prepend the
    #        command with a '-' sign to ignore its exit code

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:3777":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:3777": (tmpdir "tests")
    just p1::build
    just p1::tmpdir "tests"
    just "p1::test{{num}}" "{{server}}"  \
        | tee "/tmp/madkv-p1/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:3777": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:3777": (tmpdir "bench")
    just p1::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p1::client "{{server}}" \
        | tee "/tmp/madkv-p1/bench/bench-{{nclis}}-{{wload}}.log"
    just p1::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj1.py
