#include "raft.h"
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <random>
#include <string>

int getRandom(int min, int max) {
  std::random_device rd;  // Non-deterministic random seed
  std::mt19937 gen(rd()); // Mersenne Twister RNG
  std::uniform_int_distribution<> distr(min, max);
  return distr(gen);
}

Raft::Raft() { std::cout << "In default constructor of Raft"; }

Raft::Raft(std::string &peer_addresses, int replica_id) {
  replicaId = replica_id;
  if (replica_id == 0) {
    role = Role::LEADER;
  }
  electionTimeout = getRandom(10000, 22000);
  lastTimeWhenReceivedRPC = currentTimestampMillis();
}

uint64_t Raft::currentTimestampMillis() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch())
      .count();
}