set working-directory := '..'
set shell := ["bash","-cu"]
# list project 3 just recipes
default:
    @just --list p3 --unsorted

# install extra dependencies of your KV code
deps:
    just p2::deps

# build your executables in release mode
build:
    clang-format -i impl/include/*.h && \
    clang-format -i impl/src/*.cc && \
    just p3::build-proto && \
    just p3::build-cmake
    
# build your executables in release mode
build-proto:
    # Step 1: Generate the C++ header and cc files using protoc
    cd ./impl && \
    mkdir -p generated && \
    protoc -I=protos --grpc_out=generated --cpp_out=generated --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ./protos/kvstore.proto ./protos/clustermanagerservice.proto ./protos/raft.proto 

build-cmake:
    # From madkv directory
    pwd
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

# run your KV store manager replica
manager man_port="3666" \
        server_rf="5" \
        servers="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711"\
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        rep_id="0" \
        backer_path="./backer.m.0":
    just p3::build
    rm -f backer.*
    ./impl/cmake/build/clustermanager {{man_port}} {{servers}} {{server_rf}} 
    # FIXME: your manager run command here, listening on 0.0.0.0:p2p_port for
    #        peer replica connections and 0.0.0.0:man_port for both server and
    #        client registration requests

manager-2partitions-2rf:
    just p3::manager 3666 2 127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710 

manager-2partitions-3rf:
    just p3::manager 3666 3 127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3711,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3712 

# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666" \
       api_port="127.0.0.1:3777" \
       p2p_port="127.0.0.1:3707" \
       peers="127.0.0.1:3708" \
       backer_path="./backer.s0.0":
    clear
    ./impl/cmake/build/serverimpl {{part_id}} {{managers}} {{p2p_port}} {{backer_path}} {{peers}} {{rep_id}}
    # FIXME: your server run command here. The server should first contact the
    #        replicated managers to get its assignment of key range(s). It then
    #        listens on 0.0.0.0:p2p_port for peer replica connections and
    #        0.0.0.0:api_port for new clients, and stores all durable state under
    #        backer_path

# run you KV store client in stdin/out interface mode
client managers="127.0.0.1:3666" input="":
    #just p3::build 
    if [ -n "{{input}}" ]; then \
        cat "{{input}}" | ./impl/cmake/build/clientimpl "{{managers}}"; \
    else \
        ./impl/cmake/build/clientimpl "{{managers}}"; \
    fi
    #./impl/cmake/build/clientimpl "{{managers}}"; 
    
    # FIXME: your client run command here, querying the replicated managers
    #        for server registration information, and then connects to the
    #        servers ready to perform operations

# kill all processes of your KV store system
kill:
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
tmpdir_prefix := "/tmp/madkv-p3"

# launch the KV service components on node (see README for conventions)
service node="s0.0" \
        managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
        manager_p2ps="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782" \
        server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712" \
        backer_prefix="./backer":
    just p3::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p3::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            "{{managers}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py portof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py peersof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p3::manager \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{managers}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{manager_p2ps}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py peersof "{{manager_p2ps}}" "{{node}}") \
            "{{server_rf}}" \
            "{{servers}}" \
            "{{backer_prefix}}.{{node}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz server_rf="5" crashing="yes" \
     managers="127.0.0.1:3666": (tmpdir "fuzz")
    # just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666": (tmpdir "bench")
    #just p3::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}" \
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py

test4 manager="127.0.0.1:3666":
    just p1::check_file_exists "./tests/test2_conflicting.txt"
    just p1::check_file_exists "./tests/test2.txt"
    just p3::client {{manager}} "./tests/test2_conflicting.txt" & 
    just p3::client {{manager}} "./tests/test2.txt" &
    wait  # Ensures both clients complete