set working-directory := '..'
set shell := ["bash","-cu"]
# list project 2 just recipes
default:
    @just --list p2 --unsorted

# install extra dependencies of your KV code
deps:
    export MY_INSTALL_DIR=$HOME/.local && \
    mkdir -p $MY_INSTALL_DIR && \
    export PATH="$MY_INSTALL_DIR/bin:$PATH" && \
    sudo apt install -y cmake && \
    cmake --version && \
    cd .. && \
    \
    sudo apt install -y build-essential autoconf libtool pkg-config sqlite3 libsqlite3-dev clang-format && \
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

# build your executables in release mode
build:
    just p2::build-proto && \
    just p2::build-cmake

# build your executables in release mode
build-proto:
    # Step 1: Generate the C++ header and cc files using protoc
    cd ./impl && \
    mkdir -p generated && \
    protoc -I=protos --grpc_out=generated --cpp_out=generated --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ./protos/kvstore.proto ./protos/clustermanagerservice.proto

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
    rm backer.db.*

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    just p2::build
    rm -f backer.db.*
    ./impl/cmake/build/clustermanager {{man_port}} {{servers}}

# launch a long-running KV store service
single-server listen="0.0.0.0:3777":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="127.0.0.1:3777" backer_path="./backer.db.0":
    #just p2::build
    ./impl/cmake/build/serverimpl {{id}} {{manager}} {{api_port}} {{backer_path}}
    # FIXME: your server run command here. The server should first contact the
    #        manager to get its assignment of key range(s). It then listens on
    #        0.0.0.0:api_port for new clients, and stores all durable state
    #        under backer_path

database listen="0.0.0.0:50051":
    # Deprecated.
    just p2::build
    ./impl/cmake/build/dbserviceimpl {{listen}}

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666" input="":
    #just p2::build
    if [ -n "{{input}}" ]; then \
        cat "{{input}}" | ./impl/cmake/build/clientimpl "{{manager}}"; \
    else \
        ./impl/cmake/build/clientimpl "{{manager}}"; \
    fi
    # FIXME: your client run command here, querying manager at addr for server
    #        registration information

# kill all processes of your KV store system
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
tmpdir_prefix := "/tmp/madkv-p2"

# launch the KV service components on node (see README for conventions)
service node="s0" manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p2::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            "{{manager}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p2::manager \
            $({{python_run}} justmod/xtract.py portof "{{manager}}" "{{node}}") \
            "{{servers}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz nservers="1" crashing="no" manager="127.0.0.1:3666": (tmpdir "fuzz")
    just p2::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nservers}}-{{crashing}}.log"
    # just p2::kill

# run a YCSB benchmark workload
bench nclis wload nservers="1" manager="127.0.0.1:3666": (tmpdir "bench")
    just p2::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p2::client "{{manager}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{nservers}}.log"
    just p2::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj2.py
