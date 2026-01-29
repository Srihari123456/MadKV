cmake_minimum_required(VERSION 3.8)

project(HelloWorld C CXX)
include(./common.cmake)

include(FetchContent)

# Download GoogleTest at configure time
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/heads/main.zip
)
# Enable testing
enable_testing()

# Add GTest to your project
FetchContent_MakeAvailable(googletest)

# Create your test executable
add_executable(kvstore_test src/kvstore_test.cc)
target_link_libraries(kvstore_test
    gtest_main
    gmock
    gRPC::grpc++
    kvstore_grpc_proto
)

include(GoogleTest)
gtest_discover_tests(kvstore_test)

# Proto files
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/protos/*.proto")

foreach(PROTO ${PROTO_FILES})
  get_filename_component(PROTO_NAME ${PROTO} NAME_WE)
  get_filename_component(PROTO_PATH ${PROTO} PATH)

  set(${PROTO_NAME}_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc")
  set(${PROTO_NAME}_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h")
  set(${PROTO_NAME}_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc")
  set(${PROTO_NAME}_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h")

  add_custom_command(
    OUTPUT ${${PROTO_NAME}_proto_srcs} ${${PROTO_NAME}_proto_hdrs} ${${PROTO_NAME}_grpc_srcs} ${${PROTO_NAME}_grpc_hdrs}
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
         --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
         -I "${PROTO_PATH}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         "${PROTO}"
    DEPENDS "${PROTO}"
  )

  set(${PROTO_NAME}_grpc_proto
    ${${PROTO_NAME}_grpc_srcs}
    ${${PROTO_NAME}_grpc_hdrs}
    ${${PROTO_NAME}_proto_srcs}
    ${${PROTO_NAME}_proto_hdrs}
  )

  add_library(${PROTO_NAME}_grpc_proto ${${PROTO_NAME}_grpc_proto})
  target_link_libraries(${PROTO_NAME}_grpc_proto
    absl::check
    ${_REFLECTION}
    ${_GRPC_GRPCPP}
    ${_PROTOBUF_LIBPROTOBUF})
endforeach()

# Include dirs
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/generated
)

# Find SQLite3 and Threads
find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)

# Define source files
set(SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(dbserviceimpllib ${SRC_DIR}/dbserviceimpl.cc)
target_link_libraries(dbserviceimpllib
  SQLite::SQLite3 absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_library(dbClientLib ${SRC_DIR}/dbClient.cc)
target_link_libraries(dbClientLib
  dbserviceimpllib kvstore_grpc_proto SQLite::SQLite3
  absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_library(clusterManagerLib ${SRC_DIR}/clustermanager.cc)
target_link_libraries(clusterManagerLib
  dbserviceimpllib kvstore_grpc_proto clustermanagerservice_grpc_proto SQLite::SQLite3
  absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_library(raftLib ${SRC_DIR}/raft.cc)
target_link_libraries(raftLib
  dbserviceimpllib kvstore_grpc_proto raft_grpc_proto clustermanagerservice_grpc_proto SQLite::SQLite3
  absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_library(serverLib ${SRC_DIR}/serverimpl.cc)
target_link_libraries(serverLib
  raftLib dbserviceimpllib kvstore_grpc_proto raft_grpc_proto clustermanagerservice_grpc_proto SQLite::SQLite3
  absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

add_library(clientLib ${SRC_DIR}/clientimpl.cc)
target_link_libraries(clientLib
  kvstore_grpc_proto clustermanagerservice_grpc_proto SQLite::SQLite3
  absl::check absl::flags absl::flags_parse absl::log
  ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

# Executables
foreach(_target dbTestClient clustermanager serverimpl clientimpl)
  add_executable(${_target} ${SRC_DIR}/${_target}.cc)
  target_link_libraries(${_target}
    clientLib serverLib dbClientLib raftLib
    kvstore_grpc_proto clustermanagerservice_grpc_proto raft_grpc_proto
    SQLite::SQLite3
    absl::check absl::flags absl::flags_parse absl::log
    ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})
endforeach()
