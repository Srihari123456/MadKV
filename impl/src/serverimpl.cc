#include "serverimpl.h"

KeyValueStoreServiceImpl::KeyValueStoreServiceImpl(
    std::string &server_address, std::string &backer_path,
    std::shared_ptr<Channel> channel, std::string &peer_addresses,
    int partitionId, int replicaId)
    : dbClient(backer_path),
      clusterManagerStub_(ClusterManagerService::NewStub(channel)),
      raft_(peer_addresses, replicaId) {
  currentServerIp = server_address;
  // Setting up the Raft Cluster
  std::stringstream ss(peer_addresses);
  std::string ip;
  while (std::getline(ss, ip, ',')) {
    if (ip == "null")
      break;
    raftClusterStub_.push_back(keyvalue::KeyValueStore::NewStub(
        grpc::CreateChannel(ip, grpc::InsecureChannelCredentials())));
  }
  // Assumed no new follower will join, static followers decided at
  // initialization

  // Registering with the Cluster Manager
  while (true) {
    ServiceRegistrationRequest request;
    ServiceRegistrationResponse reply;
    ClientContext context;
    // Attempt to connect to the cluster manager
    request.set_ipaddress(server_address);
    request.set_partitionid(partitionId);
    Status status =
        clusterManagerStub_->RegisterServer(&context, request, &reply);

    // If the server is unavailable, retry indefinitely with a wait time
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      std::cerr << "Cluster Manager unavailable. Retrying in "
                << retry_interval.count() << " seconds..." << std::endl;
      std::this_thread::sleep_for(
          retry_interval); // Wait for the specified interval before retrying
    } else if (status.ok()) {
      // If the request is successful, exit the loop
      std::cout << "Connected successfully." << std::endl;
      break;
    } else {
      // For other types of errors, print the error message and exit
      std::cerr << "Error: " << status.error_message() << std::endl;
      break; // Exit on any non-UNAVAILABLE error
    }
  }

  // Populating the State machine
  dbClient.populateInitialKVStore(store, "!", "}");

  if (raft_.role == Role::LEADER) {
    initializeLeader();
  } else {
    startElectionTimeoutThread();
  }
}

KeyValueStoreServiceImpl::~KeyValueStoreServiceImpl() {
  stopElectionThread = true;
  if (electionThread.joinable()) {
    electionThread.join();
  }
  if (heartbeatThread.joinable()) {
    heartbeatThread.join();
  }
}

void KeyValueStoreServiceImpl::initializeLeader() {
  raft_.role = Role::LEADER;
  raft_.nextIndex.resize(raftClusterStub_.size() + 1, raft_.entries.size());
  raft_.matchIndex.resize(raftClusterStub_.size() + 1, 0);
  stopElectionThread = true;
  raft_.leaderIp = currentServerIp;
  raft_.leaderId = raft_.replicaId;
  std::cout
      << "LEADER found.ReplicaId:" << raft_.replicaId
      << ". Now will send No-OP Append ENtries RPC to establish leadership.";
  // Invoke AppendEntries RPC.
  if (raft_.commitIndex != -1) {
    // Not done for the 1st leader.
    commitAllPreviousUncommittedEntries(raft_.commitIndex,
                                        raft_.entries.size());
  }
  AppendEntries("NO-OP");
  sendPeriodicHeartbeatThread();
}

void KeyValueStoreServiceImpl::startElectionTimeoutThread() {
  electionThread = std::thread([this]() {
    while (!stopElectionThread) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(100)); // check every 100ms

      uint64_t now = raft_.currentTimestampMillis();
      uint64_t diff = now - raft_.lastTimeWhenReceivedRPC.load();
      // std::cout << "Started Election Thread. Now:" << now
      //           << " last time when received rpc"
      //           << raft_.lastTimeWhenReceivedRPC.load()
      //           << " Diff between both" << diff << std::endl;
      if (diff > raft_.electionTimeout) {
        // std::cout
        //     << "Election timeout threshold exceeded. Now calling
        //     requestVote."
        //     << std::endl;
        RequestVote(now);
      }
    }
  });
}

void KeyValueStoreServiceImpl::sendPeriodicHeartbeatThread() {
  heartbeatThread = std::thread([this]() {
    std::cout << "stopElectionThread = " << stopElectionThread << "\n";
    while (stopElectionThread && !isLeaderBlocked) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(raft_.electionTimeout / 3));
      // std::cout << "Sending Periodic Heartbeat." << std::endl;
      SendHeartbeat();
    }
  });
}

// Function to check if the string is valid UTF-8 and print raw bytes if not
bool KeyValueStoreServiceImpl::is_valid_utf8(const std::string &str) {
  // try {
  //   // Attempt to convert to a wide string using UTF-8 codec
  //   std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
  //   std::wstring wide_str = converter.from_bytes(str);

  //   // If we reach here, the string is valid UTF-8
  //   return true;
  // } catch (const std::range_error &e) {
  //   // If an exception is thrown, it means the string is not valid UTF-8
  //   std::cerr << "Invalid UTF-8 detected! Raw bytes: ";

  //   // Print raw bytes
  //   for (unsigned char c : str) {
  //     std::cerr << std::hex << (int)c << " ";
  //   }
  //   std::cerr << std::dec << std::endl;

  //   return false;
  // }
  return true;
}
int KeyValueStoreServiceImpl::findMaximumCommitIndex() {
  // std::cout << "Printing match index array for leader: ";
  // for (int i : raft_.matchIndex) {
  //   std::cout << i << " ";
  // }
  // std::cout << std::endl;
  // std::cout << "Printing next index array for leader: ";
  // for (int i : raft_.nextIndex) {
  //   std::cout << i << " ";
  // }
  // std::cout << std::endl;

  int prevCommitIndex = raft_.commitIndex;
  std::vector<int> sortedMatchIndex = raft_.matchIndex;

  // Sort matchIndex to efficiently find majority condition
  std::sort(sortedMatchIndex.begin(), sortedMatchIndex.end());

  int majority = (sortedMatchIndex.size() / 2) + 1; // Majority count
  int maxMatchIndex = sortedMatchIndex.back();      // Maximum matchIndex value
  int newCommitIndex = prevCommitIndex;

  // Iterate from highest possible N down to prevCommitIndex
  for (int i = maxMatchIndex; i > prevCommitIndex; --i) {
    // Count how many indices in matchIndex are >= i
    int count = std::count_if(sortedMatchIndex.begin(), sortedMatchIndex.end(),
                              [i](int x) { return x >= i; });

    if (count >= majority) {
      newCommitIndex =
          i; // Found the maximum N satisfying the majority condition
      break;
    }
  }

  return newCommitIndex;
}
Status KeyValueStoreServiceImpl::PutKey(std::string key, std::string value,
                                        KeyValueResponse *reply) {

  if (!is_valid_utf8(key) || !is_valid_utf8(value)) {
    std::cerr << "received from client:Invalid UTF-8 detected in PUT key: "
              << key << std::endl;
    std::cerr << "Rcvd from client:Invalid UTF-8 detected in PUT key: " << value
              << std::endl;
  }

  // dbClient.InsertEntry(static_cast<int64_t>(std::time(nullptr)), "PUT",
  // key,
  //                      value);
  bool exists = (store.find(key) != store.end());
  store[key] = value;

  reply->set_success(exists);
  return Status::OK;
}
Status KeyValueStoreServiceImpl::SwapKey(std::string key, std::string new_value,
                                         KeyValueResponse *reply) {

  if (!is_valid_utf8(key) || !is_valid_utf8(new_value)) {
    std::cerr << "received from client:Invalid UTF-8 detected in SWP key: "
              << key << std::endl;
    std::cerr << "Rcvd from client:Invalid UTF-8 detected in SWP key: "
              << new_value << std::endl;
  }
  // dbClient.InsertEntry(static_cast<int64_t>(std::time(nullptr)), "SWAP",
  // key,
  //                      new_value);

  auto it = store.find(key);

  if (it != store.end()) {
    std::string old_value = it->second;
    if (!is_valid_utf8(old_value) || !is_valid_utf8(key) ||
        !is_valid_utf8(it->second) || !is_valid_utf8(it->first)) {
      std::cerr << "Invalid UTF-8 detected in SWAP key: " << it->second
                << " key:" << key << " " << old_value << std::endl;
    }
    reply->set_success(true);
    reply->set_value(old_value); // Return old value
  } else {
    reply->set_success(false);
    reply->set_value("null"); // Key not found, return "null"
  }

  store[key] = new_value;
  return Status::OK;
}
Status KeyValueStoreServiceImpl::DeleteKey(std::string key,
                                           KeyValueResponse *reply) {
  // dbClient.InsertEntry(static_cast<int64_t>(std::time(nullptr)), "DELETE",
  //                      key, "");

  auto it = store.find(key);
  if (it != store.end()) {
    store.erase(it);
    reply->set_success(true);
  } else {
    reply->set_success(false);
  }

  return Status::OK;
}
Status KeyValueStoreServiceImpl::GetKey(std::string key,
                                        KeyValueResponse *reply) {

  if (!is_valid_utf8(key)) {
    std::cerr << "received from client:Invalid UTF-8 detected in GET key: "
              << key << std::endl;
  }
  auto it = store.find(key);
  if (it != store.end()) {
    reply->set_success(true);
    if (!is_valid_utf8(it->second)) {
      std::cerr << "Invalid UTF-8 detected in GET key: " << it->second << " "
                << it->first << std::endl;
    }
    reply->set_value(it->second); // Return the found value
  } else {
    reply->set_success(false);
    reply->set_value("null"); // Key not found
  }

  return Status::OK;
}
Status KeyValueStoreServiceImpl::ScanKey(std::string startkey,
                                         std::string endkey,
                                         ScanKeyValueResponse *reply) {

  if (!is_valid_utf8(startkey) || !is_valid_utf8(endkey)) {
    std::cerr << "Invalid UTF-8 detected in SCAN key: "
              << " key:" << startkey << " " << endkey << std::endl;
  }
  auto it_start = store.lower_bound(startkey); // First key >= startKey
  auto it_end = store.upper_bound(endkey);     // First key > endKey (exclusive)

  for (auto it = it_start; it != it_end && it != store.end(); ++it) {
    if (!is_valid_utf8(it->first)) {
      std::cerr << "Invalid UTF-8 detected in SCAN key: " << it->first
                << std::endl;
    }

    // Check if value is valid UTF-8
    if (!is_valid_utf8(it->second)) {
      std::cerr << "Invalid UTF-8 detected in SCAN value: " << it->second
                << std::endl;
    }
    reply->add_scanresponsekeys(it->first);
    reply->add_scanresponsevalues(it->second);
  }

  return Status::OK;
}
void KeyValueStoreServiceImpl::sendHeartbeatToFollower(int followerId) {
  ClientContext clientContext;
  HeartbeatRequest request;
  HeartbeatResponse reply;

  request.set_leaderid(raft_.replicaId);
  // std::cout << "\nSent Heartbeat\n";
  raftClusterStub_[followerId]->Heartbeat(&clientContext, request, &reply);
}
void KeyValueStoreServiceImpl::SendHeartbeat() {
  std::vector<std::thread> threads;
  // std::cout<<"Creating new threads for each replica\n";
  for (size_t i = 0; i < raftClusterStub_.size(); ++i) {
    threads.emplace_back(&KeyValueStoreServiceImpl::sendHeartbeatToFollower,
                         this, i);
  }

  for (auto &t : threads) {
    t.join();
  }
}
void KeyValueStoreServiceImpl::commitAllPreviousUncommittedEntries(
    int prevCommitIndex, int maxCommitIndex) {
  if (maxCommitIndex > raft_.commitIndex) {
    raft_.commitIndex =
        std::min((int64_t)maxCommitIndex, (int64_t)raft_.entries.size() - 1);
  }
  for (int i = prevCommitIndex; i <= raft_.commitIndex; i++) {
    std::string command = raft_.entries[i].command;
    std::istringstream iss(command);
    std::vector<std::string> tokens;
    std::string token;

    // Split command by space
    while (iss >> token) {
      tokens.push_back(token);
    }

    if (tokens.empty())
      continue; // Skip empty commands

    std::string op = tokens[0]; // First token is the operation
    std::string key = (tokens.size() > 1) ? tokens[1] : "";
    std::string val = (tokens.size() > 2) ? tokens[2] : "";
    // std::cout << "Op" << op << "  " << key << " " << val << " "
    //           << prevCommitIndex << " " << std::endl;
    if (op == "PUT") {
      KeyValueResponse putOpReply;
      PutKey(key, val, &putOpReply);
    } else if (op == "DELETE") {
      KeyValueResponse deleteOpReply;
      DeleteKey(key, &deleteOpReply);
    } else if (op == "SWAP") {
      KeyValueResponse swapOpReply;
      SwapKey(key, val, &swapOpReply);
    } else if (op == "GET") {
      KeyValueResponse getOpReply;
      GetKey(key, &getOpReply);
    } else if (op == "SCAN") {
      ScanKeyValueResponse scanOpReply;
      ScanKey(key, val, &scanOpReply);
    } else if (op == "NO-OP") {
      // std::cout << "NO-OP operation: " << std::endl;
    } else {
      std::cerr << "Unknown operation: " << op << std::endl;
    }
  }
}

// sender implementation of request vote.
void KeyValueStoreServiceImpl::RequestVote(uint64_t now) {
  raft_.role = Role::CANDIDATE;
  raft_.currentTerm = raft_.currentTerm + 1;
  raft_.lastTimeWhenReceivedRPC = now; // Reset timer so it doesn't keep firing

  std::vector<std::thread> threads;
  std::atomic<int> countOfVotesReceived = 1;
  // std::cout << "Sending request vote RPC to all servers. Current term:"
  //           << raft_.currentTerm << " last time when received rpc"
  //           << raft_.lastTimeWhenReceivedRPC << " " << std::endl;
  for (size_t i = 0; i < raftClusterStub_.size(); ++i) {
    threads.emplace_back(
        &KeyValueStoreServiceImpl::SendRequestVoteRPCToAllServers, this, i,
        &countOfVotesReceived);
  }

  for (auto &t : threads) {
    t.join();
  }
  // More than 50% of votes required to change role to LEADER
  if (countOfVotesReceived.load() > (raftClusterStub_.size()) / 2) {
    std::cout << "Majority received for " << raft_.replicaId
              << ". Will Become leader. Current term:" << raft_.currentTerm
              << std::endl;

    // send no op appendentriesRPC, stoplelctionTimeout=true;matchIndex,
    // nextIndex array reinitalize, ti
    initializeLeader();
  }
}
void KeyValueStoreServiceImpl::SendRequestVoteRPCToAllServers(
    int followerId, std::atomic<int> *countOfVotesReceived) {

  // Keep retrying until success
  ClientContext clientContext;
  RequestVoteType request;
  RequestVoteResponse reply;

  // Populate request
  request.set_term(raft_.currentTerm);
  request.set_candidateid(raft_.replicaId);
  if (raft_.entries.size() - 1 < 0) {
    request.set_lastlogindex(-1);
    request.set_lastlogterm(0);
  } else {
    request.set_lastlogindex(raft_.entries[raft_.entries.size() - 1].index);
    request.set_lastlogterm(raft_.entries[raft_.entries.size() - 1].term);
  }

  // std::lock_guard<std::mutex> lock(coutMutex);
  //  std::cout << "RequestVote: For followerId" << followerId
  //            << " inside thread function, about to call follower replica "
  //               "request vote with params"
  //            << raft_.currentTerm << " lastlogindex:" <<
  //            request.lastlogindex()
  //            << " lastlogterm:" << request.lastlogterm() << std::endl;

  Status status = raftClusterStub_[followerId]->RequestVote(&clientContext,
                                                            request, &reply);

  if (!status.ok()) {
    std::cerr << "RequestVote RPC failed for follower " << followerId
              << std::endl;
    return; // Exit if there's a network failure
  }

  // // Process response
  int followerTerm = reply.term();
  std::cout << "reply.term from follower" << followerId
            << " replyTerm:" << reply.term()
            << " Vote granted:" << reply.votegranted() << std::endl;
  if (followerTerm > raft_.currentTerm) {
    // Follower has a newer term, step down as leader
    raft_.currentTerm = followerTerm;
    raft_.role = Role::FOLLOWER;
    stopElectionThread = false;
    startElectionTimeoutThread();
    sendPeriodicHeartbeatThread();
    return;
  }

  if (reply.votegranted()) {
    countOfVotesReceived->fetch_add(1);
    return; // Exit loop if successful
  } else {
    std::cout << "Vote not granted from followerId:" << followerId << " "
              << reply.term() << " " << reply.votegranted() << std::endl;
  }
}

Status KeyValueStoreServiceImpl::PutKey(ServerContext *context,
                                        const KeyValueRequest *request,
                                        KeyValueResponse *reply) {

  std::string key(request->key().begin(), request->key().end());
  std::string value(request->value().begin(), request->value().end());

  if (raft_.role == Role::LEADER) {
    // Invoke AppendEntries RPC.
    AppendEntries("PUT " + key + " " + value);
  } else { //(raft_.role == Role::FOLLOWER /**src of request is client*/) {
    std::cout << "I am not the leader. New leader id is:" << raft_.leaderId
              << ".Please contact new leader" << std::endl;
    reply->set_success(false);
    reply->set_value("Leader not found. Please try at leader ip in response.");
    reply->set_leaderid(raft_.leaderId);
    return grpc::Status(grpc::StatusCode::OK, "Key not found");
  }
  Status status = PutKey(key, value, reply);
  return status;
}

Status KeyValueStoreServiceImpl::SwapKey(ServerContext *context,
                                         const KeyValueRequest *request,
                                         KeyValueResponse *reply) {

  std::string key(request->key().begin(), request->key().end());
  std::string new_value(request->value().begin(), request->value().end());
  if (raft_.role == Role::LEADER) {
    // Invoke AppendEntries RPC.
    AppendEntries("SWAP " + key + " " + new_value);
  } else /**src of request is client*/ {
    // redirection to client with LEADER IP.
    std::cout << "I am not the leader. New leader id is:" << raft_.leaderId
              << ".Please contact new leader" << std::endl;
    reply->set_success(false);
    reply->set_value("Leader not found. Please try at leader ip in response.");
    reply->set_leaderid(raft_.leaderId);
    return grpc::Status(grpc::StatusCode::OK, "Key not found");
  }
  Status status = SwapKey(key, new_value, reply);
  return status;
}

Status KeyValueStoreServiceImpl::GetKey(ServerContext *context,
                                        const KeyValueRequest *request,
                                        KeyValueResponse *reply) {

  std::string key(request->key().begin(), request->key().end());

  if (raft_.role == Role::LEADER) {
    // Invoke AppendEntries RPC.
    AppendEntries("GET " + key);
  } else {
    std::cout << "I am not the leader. New leader id is:" << raft_.leaderId
              << ".Please contact new leader" << std::endl;
    reply->set_success(false);
    reply->set_value("Leader not found. Please try at leader ip in response.");
    reply->set_leaderid(raft_.leaderId);
    return grpc::Status(grpc::StatusCode::OK, "Key not found");
  }
  return GetKey(key, reply);
}

Status KeyValueStoreServiceImpl::ScanKey(ServerContext *context,
                                         const KeyValueRequest *request,
                                         ScanKeyValueResponse *reply) {

  std::string startkey(request->key().begin(), request->key().end());
  std::string endkey(request->value().begin(), request->value().end());
  if (raft_.role == Role::LEADER) {
    AppendEntries("SCAN " + startkey + " " + endkey);
  } else {
    std::cout << "I am not the leader. New leader id is:" << raft_.leaderId
              << ".Please contact new leader" << std::endl;
    reply->set_leaderid(raft_.leaderId);
    return grpc::Status(grpc::StatusCode::OK, "Key not found");
  }
  return ScanKey(startkey, endkey, reply);
}

Status KeyValueStoreServiceImpl::DeleteKey(ServerContext *context,
                                           const KeyValueRequest *request,
                                           KeyValueResponse *reply) {

  std::string key(request->key().begin(), request->key().end());
  if (raft_.role == Role::LEADER) {
    // Invoke AppendEntries RPC.
    AppendEntries("DELETE " + key);
  } else {
    // redirection to client with LEADER IP.
    std::cout << "I am not the leader. New leader id is:" << raft_.leaderId
              << ".Please contact new leader" << std::endl;
    reply->set_success(false);
    reply->set_value("Leader not found. Please try at leader ip in response.");
    reply->set_leaderid(raft_.leaderId);
    return grpc::Status(grpc::StatusCode::OK, "Key not found");
  }
  Status status = DeleteKey(key, reply);
  return status;
}
void KeyValueStoreServiceImpl::sendAppendEntriesToFollower(int followerId) {
  int cnt = 0;
  while (true) { // Keep retrying until success
    ClientContext clientContext;
    AppendEntriesRequest request;
    AppendEntriesResponse reply;

    // Populate request
    request.set_term((raft_.currentTerm));
    request.set_leaderid(raft_.replicaId);
    request.set_leaderip(currentServerIp);
    // first no-op for first elected leader
    if (raft_.nextIndex[followerId] - 1 < 0) {
      request.set_prevlogindex(-1);
      request.set_prevlogterm(0);
    } else {
      request.set_prevlogindex(
          (raft_.entries[raft_.nextIndex[followerId] - 1].index));
      request.set_prevlogterm(
          (raft_.entries[raft_.nextIndex[followerId] - 1].term));
    }
    request.set_leadercommit((raft_.commitIndex));

    // Send all log entries from nextIndex[followerId] onwards
    for (int i = raft_.nextIndex[followerId]; i < raft_.entries.size(); ++i) {
      request.add_entriesindex((raft_.entries[i].index));
      request.add_entriesterm((raft_.entries[i].term));
      request.add_entriescommand(raft_.entries[i].command);
      // std::cout << "\nAdded in AppendEntriesRequest"
      //           << " the command:" << raft_.entries[i].command
      //           << "for follower" << followerId << std::endl;
    }
    // std::lock_guard<std::mutex> lock(coutMutex);
    // std::cout << "For followerId" << followerId
    //           << " inside thread function, about to call follower replica "
    //              "append entries with commit index "
    //           << " " << raft_.commitIndex << "loopCnt:" << cnt <<
    //           std::endl;
    // Call AppendEntries RPC
    Status status = raftClusterStub_[followerId]->AppendEntries(
        &clientContext, request, &reply);
    cnt += 1;
    // std::cout << cnt << "loop execution of followerId" << followerId
    //           << " completed " << std::endl;
    if (!status.ok()) {
      std::cerr << "AppendEntries RPC failed for follower " << followerId
                << std::endl;
      return; // Exit retry loop if there's a network failure
    }

    // Process response
    int followerTerm = reply.term();
    // std::cout << "FollowerId:" << followerId
    //           << " noOfTimesLoopExecuted:" << cnt
    //           << " replyTerm:" << reply.term()
    //           << " replySuccess:" << reply.success() << std::endl;
    if (followerTerm > raft_.currentTerm) {
      // Follower has a newer term, step down as leader
      raft_.currentTerm = followerTerm;
      raft_.role = Role::FOLLOWER;
      stopElectionThread = false;
      startElectionTimeoutThread();
      sendPeriodicHeartbeatThread();
      return;
    }

    if (reply.success()) {
      std::lock_guard<std::mutex> lock(raft_.matchIndexMutex);
      raft_.matchIndex[followerId] = raft_.nextIndex[followerId];
      raft_.nextIndex[followerId]++;
      return; // Exit loop if successful
    } else {
      std::cout << "Reply not success from followerId:" << followerId << " "
                << reply.term() << " " << reply.success() << std::endl;
      // Retry with previous log index
      raft_.nextIndex[followerId]--;
    }
  }
}

void KeyValueStoreServiceImpl::sendAppendEntriesToAll() {
  std::vector<std::thread> threads;
  // std::cout<<"Creating new threads for each replica\n";
  for (size_t i = 0; i < raftClusterStub_.size(); ++i) {
    threads.emplace_back(&KeyValueStoreServiceImpl::sendAppendEntriesToFollower,
                         this, i);
  }

  for (auto &t : threads) {
    t.join();
  }
}

// sender implementation - only done by leader
void KeyValueStoreServiceImpl::AppendEntries(std::string command) {
  std::lock_guard<std::mutex> lock(appendEntriesMutex);
  /*
  term: currentTerm (Raft class variable )
  leaderId:Take replica_id in input from main(), create class variable to
  send replica id of current server leaderIP : IP address of the leader in
  Raft class and method param prevLogIndex: min( log.size() - 1 ,
  nextIndex[follower] ) prevLogTerm: log[prevLogIndex].term entries: [
    log[prevLogIndex] ... log[log.size() - 1] ] ::  send all log entries from
    prevLogIndex to last, batch multiple entries for  efficiency leaderCommit:
    commitIndex (Raft Class Variable) In GET, SCAN: Check in memory map of
    leader, and respond back to client. In DELETE, SWAP, PUT methods, two
    conditions based on state of the server If server role is NOT LEADER
    (which means it is FOLLOWER or CANDIDATE): Then redirect to LEADER
    (FOLLOWER will send back ip address of LEADER to client, client will
    reinitiate a request to LEADER for the same command) If server role is
    LEADER: 1) Append command in entries local class variable of LEADER 2)
    Issue AppendEntries RPC in multiple threads for each follower with
    different parameters viz. prevLogIndex, prevLogTerm, entries 3) Set return
    type for thread with two variabels, term and success from FOLLOWER 4) Once
    this response is received on the LEADER, then two conditions: 4a) If
    response.success == TRUE, then update LEADER's matchIndex and nextIndex in
    LEADER for that particular FOLLOWER 4b) If response.success == FALSE, then
    decrement nextIdnex for that FOLLOWER and retry AppendEntries RPC call
    5)Create new variable prevCommitIndex = commitIndex. Now, iterate
    periodically e.g. Every 3s once over matchIndex array in LEADER or wait
    for all threads to join. Find maximum N such that it is greater than
    prevCommitIndex and less than maximum of matchIndex array, such that
    majority of indices in matchIndex array are greater than N. Set
    commitIndex to value N 6) Apply all commands to in memory map + SQLITE DB
    from prevCommitIndex to N 7) Respond to  client

    */
  // replicating inside leader log

  raft_.entries.push_back(
      LogEntry(raft_.entries.size(), raft_.currentTerm, command));
  // std::cout<<"Raft entry replicated in leader log\n";

  {
    std::lock_guard<std::mutex> lock(raft_.matchIndexMutex);
    raft_.matchIndex[raft_.matchIndex.size() - 1] =
        raft_.nextIndex[raft_.nextIndex.size() - 1];
    raft_.nextIndex[raft_.nextIndex.size() - 1]++;
  }

  // Send in parallel to all followers
  sendAppendEntriesToAll();

  // Check for majority after all threads have joined
  int N = findMaximumCommitIndex();
  if (N > raft_.commitIndex) {
    std::cout << "New commit index found" << N << std::endl;
    raft_.commitIndex = N;
  } else {
    // System block. no response to client. Exit
    std::cout << "Server blocked"
              << "raft_.commitIndex" << raft_.commitIndex << "N:" << N << "\n";
    // TODO: Stop sending heartbeats.
    isLeaderBlocked = true;

    while (true) {
      //
    }
  }
}

// only received at follower from leader
Status
KeyValueStoreServiceImpl::AppendEntries(ServerContext *context,
                                        const AppendEntriesRequest *request,
                                        AppendEntriesResponse *reply) {
  // std::cout << "In raft follower AppendEntries RPC" << std::endl;
  /*
    In DELETE, SWAP, PUT methods, two conditions based on state of the server
    1. Reply false if request.term <  raft class variable currentTerm .
    2. *Check historic previous entry. If that does not match, reply false.*
    Reply false if raft class variable log doesn’t contain an entry at request
    variable prevLogIndex whose term matches  request variable prevLogTerm .
    Check for negative variables. If negative request.prevLogIndex, then
    proceed to next step.
    3. *Then Check current entry.* If an existing entry conflicts with a new
    one (same index but different terms), delete the existing entry and all
    that follow it
    4. Append any new entries not already in the log. In the request received,
    check for leaderId and leaderIp and update it locally in class variables
    of follower.
    5. *Update commit index of follower, and then apply to follower state
    machine.* If leaderCommit > commitIndex, set commitIndex =
    min(leaderCommit, index of last new entry)
  */
  raft_.lastTimeWhenReceivedRPC = raft_.currentTimestampMillis();
  if (request->term() < raft_.currentTerm) {
    // std::cout << "In receiver implementation. failed append entries call as
    // Request term:" << request->term() << " and current term" <<
    // raft_.currentTerm << std::endl;
    reply->set_success(false);
    reply->set_term(raft_.currentTerm);
    return Status::OK;
  }
  // first no-op for first elected leader

  if (request->prevlogindex() < 0) {
    //
  } else if (raft_.entries.size() > request->prevlogindex() &&
             raft_.entries[request->prevlogindex()].term !=
                 request->prevlogterm()) {
    // std::cout << "In receiver implementation. failed append entries call as
    // raft entreis size :" << raft_.entries.size() << " and prevLogIndex" <<
    // request->prevlogindex() << " term:" <<
    // raft_.entries[request->prevlogindex()].term << " prevLogTerm:" <<
    // request->prevlogterm() <<  std::endl;
    reply->set_success(false);
    reply->set_term(raft_.currentTerm);
    return Status::OK;
  } else if (raft_.entries.size() <= request->prevlogindex()) {
    std::cout << "In receiver implementation. Failed append entries call as "
                 "raft entreis size :"
              << raft_.entries.size() << " and prevLogIndex"
              << request->prevlogindex() << std::endl;
    reply->set_success(false);
    reply->set_term(raft_.currentTerm);
    return Status::OK;
  }
  if (raft_.role == Role::LEADER) {
    raft_.role = Role::FOLLOWER;
    stopElectionThread = false;
    startElectionTimeoutThread();
    // sendPeriodicHeartbeatThread();
  }
  // check current entry : if doesnt match, delete it and everything that
  // follows if((raft_.entries.size() > request.prevlogindex() + 1) &&
  // raft_.entries[request.prevlogindex() + 1].term == )
  raft_.entries.resize(request->prevlogindex() + 1);
  // normal append entry
  for (int i = 0; i < request->entriesterm_size(); i++) {
    raft_.entries.push_back(LogEntry(request->entriesindex(i),
                                     request->entriesterm(i),
                                     request->entriescommand(i)));
    // std::cout << "Entry Term [" << i << "]: " << request->entriesterm(i)
    //           << std::endl;
    // std::cout << "Entry Index [" << i << "]: " << request->entriesindex(i)
    //           << std::endl;
    // std::cout << "Entry Command [" << i << "]: " <<
    // request->entriescommand(i)
    //           << std::endl;
  }
  raft_.leaderIp = request->leaderip();
  raft_.leaderId = request->leaderid();
  // set commit index

  int prevCommitIndex = raft_.commitIndex + 1;
  commitAllPreviousUncommittedEntries(prevCommitIndex, request->leadercommit());
  // std::cout << "Follower id: " << raft_.replicaId
  //           << " entries size:" << raft_.entries.size()
  //           << " leader ip:" << raft_.leaderIp
  //           << " current term of follower:" << raft_.currentTerm
  //           << " CommitIndex of follower:" << raft_.commitIndex <<
  //           std::endl;

  // return to leader
  reply->set_success(true);
  reply->set_term(raft_.currentTerm);
  return Status::OK;
}
// request vote receiver implementation
Status KeyValueStoreServiceImpl::RequestVote(ServerContext *context,
                                             const RequestVoteType *request,
                                             RequestVoteResponse *reply) {
  std::cout << "In Request Voter receiver implementation" << std::endl;
  // raft_.lastTimeWhenReceivedRPC = raft_.currentTimestampMillis();

  if (request->term() < raft_.currentTerm) {
    std::cout << "[[Candidate with smaller term]] Not granting vote, "
                 "raft_.currentTerm = "
              << raft_.currentTerm << " raft_.votedFor = " << raft_.votedFor
              << "request->term() " << request->term()
              << "raft_.replicaId = " << raft_.replicaId << "\n";
    reply->set_term(raft_.currentTerm);
    reply->set_votegranted(false);
    return Status::OK;
  }

  if (raft_.votedFor == -1 || request->term() > raft_.currentTerm) {
    // We haven't voted for any one (or this is a new term).
    if ((raft_.entries.size() - 1 >= 0) &&
        ((request->lastlogterm() <
          raft_.entries[raft_.entries.size() - 1].term) ||
         ((request->lastlogterm() ==
           raft_.entries[raft_.entries.size() - 1].term) &&
          (request->lastlogindex() <
           raft_.entries[raft_.entries.size() - 1].index)))) {
      // Don't grant the vote.
      std::cout << "[[Candidate with incomplete log]] Not granting vote. "
                   "raft_.currentTerm = "
                << raft_.currentTerm << " raft_.votedFor = " << raft_.votedFor
                << "request->candidateid() = " << request->candidateid()
                << " request->term() = " << request->term()
                << "\trequest->lastlogterm() " << request->lastlogterm()
                << "\traft_.entries[raft_.entries.size() - 1].term = "
                << raft_.entries[raft_.entries.size() - 1].term
                << "\trequest->lastlogindex() = " << request->lastlogindex()
                << "\traft_.entries[raft_.entries.size() - 1].index = "
                << raft_.entries[raft_.entries.size() - 1].index
                << "raft_.replicaId = " << raft_.replicaId << "\n";
      reply->set_term(raft_.currentTerm);
      reply->set_votegranted(false);
      return Status::OK;
    } else {
      // Grant the vote. Candidate has a more "complete" log.
      std::cout << "[[YESSS]]\n";
      reply->set_term(raft_.currentTerm);
      raft_.votedFor = request->candidateid();
      reply->set_votegranted(true);
      raft_.lastTimeWhenReceivedRPC = raft_.currentTimestampMillis();
      return Status::OK;
    }
  } else if ((raft_.votedFor == request->candidateid() &&
              raft_.currentTerm == request->term())) {
    // TODO: We already voted for the same candidate. Not granting vote again
    // Don't grant the vote.
    std::cout << "[[Same Candidate Again with same term]] Not granting vote "
                 "since already granted. raft_.currentTerm = "
              << raft_.currentTerm << " raft_.votedFor = " << raft_.votedFor
              << "request->candidateid() = " << request->candidateid()
              << " request->term() = " << request->term()
              << "raft_.replicaId = " << raft_.replicaId << "\n";
    reply->set_term(raft_.currentTerm);
    reply->set_votegranted(false);
    return Status::OK;
  } else {
    // Don't grant the vote more than once for a term.
    std::cout << "Not granting vote since already granted. raft_.currentTerm = "
              << raft_.currentTerm << " raft_.votedFor = " << raft_.votedFor
              << "raft_.replicaId = " << raft_.replicaId << "\n";
    reply->set_term(raft_.currentTerm);
    reply->set_votegranted(false);
    return Status::OK;
  }
}

Status KeyValueStoreServiceImpl::Heartbeat(ServerContext *context,
                                           const HeartbeatRequest *request,
                                           HeartbeatResponse *reply) {
  // std::cout << "\nReceived Heartbeat\n";
  raft_.lastTimeWhenReceivedRPC = raft_.currentTimestampMillis();
  raft_.leaderId = request->leaderid();
  return Status::OK;
}

void RunServer(std::string &server_address, std::string &backer_path,
               std::string &clusterManagerAddress, std::string &peerAddresses,
               int partitionId, int replicaId) {
  KeyValueStoreServiceImpl kv_service(
      server_address, backer_path,
      grpc::CreateChannel(clusterManagerAddress,
                          grpc::InsecureChannelCredentials()),
      peerAddresses, partitionId, replicaId);

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&kv_service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

int main(int argc, char **argv) {
  int partitionId = 0;
  std::string clusterManagerAddress = "localhost:3666";
  std::string server_address("0.0.0.0:3777");
  std::string backer_path("data.db");
  std::string peer_addresses("5.6.7.8:3708,9.10.11.12:3709");
  int replicaId = 0;
  if (argc > 1) {
    partitionId = std::stoi(argv[1]);
  }
  if (argc > 2) {
    clusterManagerAddress = argv[2];
  }
  if (argc > 3) {
    server_address = argv[3];
  }
  if (argc > 4) {
    backer_path = argv[4];
  }
  if (argc > 5) {
    peer_addresses = argv[5];
  }
  if (argc > 6) {
    replicaId = std::stoi(argv[6]);
  }
  RunServer(server_address, backer_path, clusterManagerAddress, peer_addresses,
            partitionId, replicaId);
  return 0;
}