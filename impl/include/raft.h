#include "../generated/raft.grpc.pb.h"
#include <atomic>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sqlite3.h>
#include <string>
#include <thread>
#include <vector>

using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using grpc::ServerWriter;
using grpc::Status;

using keyvalue::RaftService;

class LogEntry {
public:
  int index;
  int term;
  std::string command;
  LogEntry() {}
  LogEntry(int ind, int t, std::string cmd) {
    index = ind;
    term = t;
    command = cmd;
  }
};

enum class Role { FOLLOWER, LEADER, CANDIDATE };

class Raft : public RaftService::Service {
public:
  Role role = Role::FOLLOWER; // New member variable to track role
  int leaderId = 0;
  int replicaId = 0;
  std::string leaderIp;
  int votedFor = -1;
  int currentTerm = 0;
  int commitIndex = -1;
  int lastApplied = 0;
  std::vector<LogEntry> entries;
  std::vector<int> nextIndex;
  std::vector<int> matchIndex;
  int electionTimeout; // in ms
  std::atomic<uint64_t> lastTimeWhenReceivedRPC;
  std::mutex matchIndexMutex; // Protect matchIndex updates
  Raft();
  Raft(std::string &peer_addresses, int replicaId);
  uint64_t currentTimestampMillis();
};