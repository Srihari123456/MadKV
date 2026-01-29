// header file of serverimpl.cc
#include "../generated/clustermanagerservice.grpc.pb.h"
#include "../generated/kvstore.grpc.pb.h"
#include "dbClient.h"
#include "raft.h"
#include <codecvt>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <locale>
#include <memory>
#include <string>
#include <thread>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;

using keyvalue::AppendEntriesRequest;
using keyvalue::AppendEntriesResponse;
using keyvalue::ClusterManagerService;
using keyvalue::HeartbeatRequest;
using keyvalue::HeartbeatResponse;
using keyvalue::KeyValueRequest;
using keyvalue::KeyValueResponse;
using keyvalue::KeyValueStore;
using keyvalue::RequestVoteResponse;
using keyvalue::RequestVoteType;
using keyvalue::ScanKeyValueResponse;
using keyvalue::ServiceRegistrationRequest;
using keyvalue::ServiceRegistrationResponse;

const std::chrono::seconds retry_interval(3);
// include associated class and methods.
class KeyValueStoreServiceImpl final : public KeyValueStore::Service {
private:
  std::map<std::string, std::string> store; // Ordered key-value store
  DatabaseClient dbClient;
  std::unique_ptr<ClusterManagerService::Stub> clusterManagerStub_;
  Raft raft_;
  std::vector<std::unique_ptr<KeyValueStore::Stub>> raftClusterStub_;
  std::string currentServerIp;
  std::mutex opMutex;
  std::mutex appendEntriesMutex;
  std::thread electionThread;
  std::thread heartbeatThread;
  std::atomic<bool> stopElectionThread{false};
  std::atomic<bool> isLeaderBlocked{false};

  void initializeLeader();
  void startElectionTimeoutThread();
  void sendPeriodicHeartbeatThread();
  int findMaximumCommitIndex();
  void commitAllPreviousUncommittedEntries(int prevCommitIndex,
                                           int leaderCommit);
  void sendHeartbeatToFollower(int followerId);
  void SendHeartbeat();

  bool is_valid_utf8(const std::string &str);
  Status PutKey(std::string key, std::string value, KeyValueResponse *reply);
  Status SwapKey(std::string key, std::string new_value,
                 KeyValueResponse *reply);
  Status GetKey(std::string key, KeyValueResponse *reply);
  Status ScanKey(std::string startkey, std::string endkey,
                 ScanKeyValueResponse *reply);
  Status DeleteKey(std::string key, KeyValueResponse *reply);
  void sendAppendEntriesToFollower(int followerId);

  /**
  TODO ISSUES
  - Last command not committed on follower after no new entry sent from client.
  */

public:
  KeyValueStoreServiceImpl(std::string &server_address,
                           std::string &backer_path,
                           std::shared_ptr<Channel> channel,
                           std::string &peer_addresses, int partitionId,
                           int replicaId);
  ~KeyValueStoreServiceImpl();

  // sender implementation of request vote.
  void RequestVote(uint64_t now);
  void SendRequestVoteRPCToAllServers(int followerId,
                                      std::atomic<int> *countOfVotesReceived);

  Status PutKey(ServerContext *context, const KeyValueRequest *request,
                KeyValueResponse *reply);

  Status SwapKey(ServerContext *context, const KeyValueRequest *request,
                 KeyValueResponse *reply);

  Status GetKey(ServerContext *context, const KeyValueRequest *request,
                KeyValueResponse *reply);

  Status ScanKey(ServerContext *context, const KeyValueRequest *request,
                 ScanKeyValueResponse *reply);

  Status DeleteKey(ServerContext *context, const KeyValueRequest *request,
                   KeyValueResponse *reply);

  void sendAppendEntriesToAll();

  // sender implementation - only done by leader
  void AppendEntries(std::string command);

  // only received at follower from leader
  Status AppendEntries(ServerContext *context,
                       const AppendEntriesRequest *request,
                       AppendEntriesResponse *reply);
  // request vote receiver implementation
  Status RequestVote(ServerContext *context, const RequestVoteType *request,
                     RequestVoteResponse *reply);

  Status Heartbeat(ServerContext *context, const HeartbeatRequest *request,
                   HeartbeatResponse *reply);
};