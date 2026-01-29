#include "../generated/kvstore.grpc.pb.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
// using keyvalue::EmptyRequest;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;
using keyvalue::KeyValueRequest;
using keyvalue::KeyValueResponse;
using keyvalue::KeyValueStore;

class MockKeyValueStoreService : public keyvalue::KeyValueStore::Service {
public:
  MOCK_METHOD(Status, GetKey,
              (ServerContext * context, const KeyValueRequest *request,
               KeyValueResponse *reply),
              ());
};

class KeyValueStoreTest : public ::testing::Test {
protected:
  MockKeyValueStoreService mockService;
};

TEST_F(KeyValueStoreTest, GetEntrySuccess) {
  // Arrange
  ServerContext context;
  KeyValueRequest request;
  KeyValueResponse response;

  request.set_key("testKey");

  // Mock behavior
  EXPECT_CALL(mockService, GetKey(&context, &request, &response))
      .WillOnce([](ServerContext *, const KeyValueRequest *req,
                   KeyValueResponse *res) {
        res->set_success(true);
        res->set_value("testValue");
        return Status::OK;
      });

  // Act
  Status status = mockService.GetKey(&context, &request, &response);

  // Assert
  EXPECT_TRUE(status.ok());
  // EXPECT_EQ(response.success(), true);
  // EXPECT_EQ(response.value(), "testValue");
}

// TEST_F(KeyValueStoreTest, GetEntryNotFound) {
//     grpc::ServerContext context;
//     KeyRequest request;
//     DataEntry response;

//     request.set_key("missingKey");

//     EXPECT_CALL(mockService, GetEntry(&context, &request, &response))
//         .WillOnce(Return(Status(grpc::StatusCode::NOT_FOUND, "Key not
//         found")));

//     Status status = mockService.GetEntry(&context, &request, &response);

//     EXPECT_FALSE(status.ok());
//     EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
// }
