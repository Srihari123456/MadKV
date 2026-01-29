#include "../generated/clustermanagerservice.grpc.pb.h"
#include "../generated/kvstore.grpc.pb.h"
#include <algorithm> // For std::sort
#include <future>    // For std::async
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#define MIN_HASH_LENGTH 10
const std::chrono::seconds retry_interval(1);

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using keyvalue::ClientRegistrationResponse;
using keyvalue::ClusterManagerService;
using keyvalue::EmptyRequest;
using keyvalue::KeyValueRequest;
using keyvalue::KeyValueResponse;
using keyvalue::KeyValueStore;
using keyvalue::ScanKeyValueResponse;

class KeyValueStoreClient {
public:
  KeyValueStoreClient(std::shared_ptr<Channel> channel);
  void Initialize();
  void updateLeaderOnRetry(std::string op, int partitionId,
                           int &serverReplicaIdToContact);

  void PutKey(const std::string &key, const std::string &new_value,
              int partitionId);

  void SwapKey(const std::string &key, const std::string &new_value,
               int partitionId);

  void GetKey(const std::string &key, int partitionId);

  void DeleteKey(const std::string &key, int partitionId);

  void ScanKey(const std::string &startKey, const std::string &endKey);

  void ListenForCommands();

private:
  // vector of [ip, partitionid, replicaId]. It stores ip addresses of all
  // replicas
  std::vector<std::tuple<std::string, int, int>> serverIpAddresses;
  std::vector<std::unique_ptr<KeyValueStore::Stub>> serverstub_;
  std::unique_ptr<ClusterManagerService::Stub> clusterManagerStub_;
  int serverReplicationFactor;
  // This stores the replica id of the leader in each partition. So 2 different
  // indices in this vector can have same value.
  std::vector<int> leaderOfEachPartition;
  uint32_t sdbm(const std::string &data);
  uint32_t sdbm_last10(const std::string &data);

  uint32_t computeHashValue(std::string key);

  int computeServerIndex(std::string key);
};
