#include "clientimpl.h"

KeyValueStoreClient::KeyValueStoreClient(std::shared_ptr<Channel> channel)
    : clusterManagerStub_(ClusterManagerService::NewStub(channel)) {}
void KeyValueStoreClient::Initialize() {
  while (true) {
    ClientRegistrationResponse reply;
    ClientContext context;
    EmptyRequest emptyRequest;
    // Attempt to connect to the cluster manager
    Status status =
        clusterManagerStub_->RegisterClient(&context, emptyRequest, &reply);

    // If the server is unavailable, retry indefinitely with a wait time
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      std::cerr << status.error_message()
                << ". Cluster Manager unavailable. Retrying in "
                << retry_interval.count() << " seconds..." << std::endl;
      std::this_thread::sleep_for(
          retry_interval); // Wait for the specified interval before retrying
    } else if (status.ok()) {
      // If the request is successful, exit the loop
      // std::cout << "Connected successfully." << std::endl;
      serverReplicationFactor = reply.serverreplicationfactor();
      int replicaId = 0;
      int partitionId = 0;
      for (const auto &ip : reply.ipaddress()) {
        serverIpAddresses.push_back({ip, partitionId, replicaId});
        // std::cout << "IP:" << ip << " PartitionId:" << partitionId
        //           << " ReplicaId:" << replicaId << std::endl;
        serverstub_.push_back(keyvalue::KeyValueStore::NewStub(
            grpc::CreateChannel(ip, grpc::InsecureChannelCredentials())));
        replicaId += 1;
        if (replicaId > serverReplicationFactor) {
          replicaId = 0;
          partitionId += 1;
        }
      }
      // assumed that at system initialization, in all partitions, the leader
      // is the first node.
      leaderOfEachPartition.resize(partitionId + 1, 0);
      break;
    } else {
      // For other types of errors, print the error message and exit
      std::cerr << "Error: " << status.error_message() << std::endl;
      break; // Exit on any non-UNAVAILABLE error
    }
  }
}
void KeyValueStoreClient::updateLeaderOnRetry(std::string op, int partitionId,
                                              int &serverReplicaIdToContact) {
  // std::cerr << op << " failed as server unavailable. Retrying in "
  //           << retry_interval.count() << " seconds..." << std::endl;
  std::this_thread::sleep_for(
      retry_interval); // Wait for the specified interval before retrying
  leaderOfEachPartition[partitionId] =
      (leaderOfEachPartition[partitionId] + 1) % serverReplicationFactor;
  serverReplicaIdToContact = partitionId * serverReplicationFactor +
                             leaderOfEachPartition[partitionId];
}

void KeyValueStoreClient::PutKey(const std::string &key,
                                 const std::string &new_value,
                                 int partitionId) {
  int serverReplicaIdToContact = leaderOfEachPartition[partitionId] +
                                 partitionId * serverReplicationFactor;
  while (true) {
    KeyValueRequest request;
    request.set_key(key);
    request.set_value(new_value);
    KeyValueResponse reply;
    ClientContext context;

    Status status = serverstub_[serverReplicaIdToContact]->PutKey(
        &context, request, &reply);
    int64_t newLeader = -1;
    std::string errorMessage = "";

    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      updateLeaderOnRetry("PUT", partitionId, serverReplicaIdToContact);
    } else if (status.ok()) {
      if (reply.has_leaderid()) {
        newLeader = reply.leaderid();
        serverReplicaIdToContact =
            partitionId * serverReplicationFactor + newLeader;
        leaderOfEachPartition[partitionId] = newLeader;
        // std::cout << "Error Code Leader NOT FOUND. New leaderId:" <<
        // newLeader
        //           << "isSuccess :" << reply.success() << std::endl;
      } else {
        std::cout << "PUT " << key << " "
                  << (reply.success() ? "found" : "not_found") << std::endl;
        break;
      }
    } else {
      std::cerr << "ERROR: PUT request failed" << std::endl;
      break;
    }
  }
}

void KeyValueStoreClient::SwapKey(const std::string &key,
                                  const std::string &new_value,
                                  int partitionId) {
  int serverReplicaIdToContact = leaderOfEachPartition[partitionId] +
                                 partitionId * serverReplicationFactor;
  while (true) {
    KeyValueRequest request;
    request.set_key(key);
    request.set_value(new_value);

    KeyValueResponse reply;
    ClientContext context;

    Status status = serverstub_[serverReplicaIdToContact]->SwapKey(
        &context, request, &reply);
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      updateLeaderOnRetry("SWAP", partitionId, serverReplicaIdToContact);
    } else if (status.ok()) {
      if (reply.has_leaderid()) {
        int newLeader = reply.leaderid();
        serverReplicaIdToContact =
            partitionId * serverReplicationFactor + newLeader;
        leaderOfEachPartition[partitionId] = newLeader;
        // std::cout << "Error Code Leader NOT FOUND. New leaderId:" <<
        // newLeader
        //           << "isSuccess :" << reply.success() << std::endl;
      } else {
        std::cout << "SWAP " << key << " "
                  << (reply.success() ? reply.value() : "null") << std::endl;
        break;
      }
    } else {
      std::cerr << "ERROR: SWAP request failed" << std::endl;
      break;
    }
  }
}

void KeyValueStoreClient::GetKey(const std::string &key, int partitionId) {
  int serverReplicaIdToContact = leaderOfEachPartition[partitionId] +
                                 partitionId * serverReplicationFactor;
  while (true) {
    KeyValueRequest request;
    request.set_key(key);

    KeyValueResponse reply;
    ClientContext context;

    Status status = serverstub_[serverReplicaIdToContact]->GetKey(
        &context, request, &reply);
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      updateLeaderOnRetry("GET", partitionId, serverReplicaIdToContact);
    } else if (status.ok()) {
      if (reply.has_leaderid()) {
        int newLeader = reply.leaderid();
        serverReplicaIdToContact =
            partitionId * serverReplicationFactor + newLeader;
        leaderOfEachPartition[partitionId] = newLeader;
        // std::cout << "Error Code Leader NOT FOUND. New leaderId:" <<
        // newLeader
        //           << "isSuccess :" << reply.success() << std::endl;
      } else {
        std::cout << "GET " << key << " "
                  << (reply.success() ? reply.value() : "null") << std::endl;
        break;
      }
    } else {
      std::cerr << "ERROR: GET request failed" << std::endl;
      break;
    }
  }
}

void KeyValueStoreClient::DeleteKey(const std::string &key, int partitionId) {
  int serverReplicaIdToContact = leaderOfEachPartition[partitionId] +
                                 partitionId * serverReplicationFactor;
  while (true) {
    KeyValueRequest request;
    request.set_key(key);
    KeyValueResponse reply;
    ClientContext context;

    Status status = serverstub_[serverReplicaIdToContact]->DeleteKey(
        &context, request, &reply);
    if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      updateLeaderOnRetry("DELETE", partitionId, serverReplicaIdToContact);
    } else if (status.ok()) {
      if (reply.has_leaderid()) {
        int newLeader = reply.leaderid();
        serverReplicaIdToContact =
            partitionId * serverReplicationFactor + newLeader;
        leaderOfEachPartition[partitionId] = newLeader;
        // std::cout << "Error Code Leader NOT FOUND. New leaderId:" <<
        // newLeader
        //           << "isSuccess :" << reply.success() << std::endl;
      } else {
        std::cout << "DELETE " << key << " "
                  << (reply.success() ? "found" : "not_found") << std::endl;
        break;
      }

    } else {
      std::cerr << "ERROR: DELETE request failed" << std::endl;
      break;
    }
  }
}

void KeyValueStoreClient::ScanKey(const std::string &startKey,
                                  const std::string &endKey) {
  std::map<std::string, std::string> combinedResults; // Final merged map
  std::mutex resultMutex; // Mutex to handle concurrent writes

  // Lambda for parallel scanning
  auto scanTask = [&](int partitionId) {
    int serverReplicaIdToContact = leaderOfEachPartition[partitionId] +
                                   partitionId * serverReplicationFactor;
    while (true) {
      KeyValueRequest request;
      request.set_key(startKey);
      request.set_value(endKey);

      ScanKeyValueResponse reply;
      ClientContext context;

      Status status = serverstub_[serverReplicaIdToContact]->ScanKey(
          &context, request, &reply);
      if (status.error_code() == grpc::StatusCode::UNAVAILABLE) {
        updateLeaderOnRetry("SCAN", partitionId, serverReplicaIdToContact);
      } else if (status.ok()) {

        if (reply.has_leaderid()) {
          int newLeader = reply.leaderid();
          serverReplicaIdToContact =
              partitionId * serverReplicationFactor + newLeader;
          leaderOfEachPartition[partitionId] = newLeader;
          // std::cout << "Error Code Leader NOT FOUND. New leaderId:"
          //           << newLeader << std::endl;
        } else {
          std::lock_guard<std::mutex> lock(
              resultMutex); // Thread-safe insertion
          for (int i = 0; i < reply.scanresponsekeys_size(); ++i) {
            const std::string &key = reply.scanresponsekeys(i);
            const std::string &value = reply.scanresponsevalues(i);
            combinedResults[key] = value;
          }
          break;
        }

      } else {
        std::cerr
            << "Error scanning from server: "
            << std::get<0>(
                   serverIpAddresses[partitionId * serverReplicationFactor])
            << status.error_message() << std::endl;
        break;
      }
    }
  };

  // Launch threads for each stub
  std::vector<std::thread> threads;
  for (int i = 0; i < serverIpAddresses.size() / serverReplicationFactor; i++) {
    threads.emplace_back(scanTask, i);
  }

  // Wait for all threads to finish
  for (auto &thread : threads) {
    thread.join();
  }

  // Sort the combined results (if necessary)
  // Already sorted if individual server maps were sorted

  // Print output in required format
  std::ostringstream response;
  response << "SCAN " << startKey << " " << endKey << " BEGIN\n";
  for (const auto &[key, value] : combinedResults) {
    response << " ";
    response << key << " " << value << "\n";
  }
  response << "SCAN END";

  std::cout << response.str() << std::endl;
}

void KeyValueStoreClient::ListenForCommands() {
  std::string line;
  while (std::getline(std::cin, line)) {
    std::istringstream iss(line);
    std::string command, key, value;
    int serverIndex = 0;
    iss >> command;

    if (command == "PUT") {
      iss >> key >> value;
      serverIndex = computeServerIndex(key);
      PutKey(key, value, serverIndex);
    } else if (command == "SWAP") {
      iss >> key >> value;
      serverIndex = computeServerIndex(key);
      SwapKey(key, value, serverIndex);
    } else if (command == "GET") {
      iss >> key;
      serverIndex = computeServerIndex(key);
      GetKey(key, serverIndex);
    } else if (command == "DELETE") {
      iss >> key;
      serverIndex = computeServerIndex(key);
      DeleteKey(key, serverIndex);
    } else if (command == "SCAN") {
      std::string startKey, endKey;
      iss >> startKey >> endKey;
      ScanKey(startKey, endKey);
    } else if (command == "STOP") {
      std::cout << "STOP" << std::endl;
      break;
    } else {
      std::cerr << "ERROR: Unknown command" << std::endl;
    }
  }
}

uint32_t KeyValueStoreClient::sdbm(const std::string &data) {
  uint32_t hash = 0;
  std::string data_substring =
      data.substr(0, std::min(MIN_HASH_LENGTH, (int)data.length()));
  for (char &c : data_substring) {
    hash = c + (hash << 6) + (hash << 16) - hash;
  }
  return hash;
}
uint32_t KeyValueStoreClient::sdbm_last10(const std::string &data) {
  uint32_t hash = 0;

  // Determine the substring to hash
  std::string data_substring =
      (data.length() >= 10)
          ? data.substr(data.length() - 10) // Last 10 characters
          : data;                           // Entire string if less than 10

  for (char c : data_substring) {
    hash = c + (hash << 6) + (hash << 16) - hash;
  }

  return hash;
}

uint32_t KeyValueStoreClient::computeHashValue(std::string key) {
  return sdbm_last10(key);
}

int KeyValueStoreClient::computeServerIndex(std::string key) {
  return computeHashValue(key) %
         (serverIpAddresses.size() / serverReplicationFactor);
}

int main(int argc, char **argv) {
  std::string clusterManagerAddress = "localhost:3666";
  if (argc > 1) {
    clusterManagerAddress = argv[1];
  }
  KeyValueStoreClient client(grpc::CreateChannel(
      clusterManagerAddress, grpc::InsecureChannelCredentials()));
  client.Initialize();
  client.ListenForCommands();
  return 0;
}
