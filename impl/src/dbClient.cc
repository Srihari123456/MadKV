#include "dbClient.h"

bool DatabaseClient::InsertEntry(int64_t timestamp, const std::string &command,
                                 const std::string &key,
                                 const std::string &value) {
  bool status = dbService.InsertEntry(timestamp, command, key, value);
  return status;
}

std::string DatabaseClient::GetEntry(const std::string &key) {

  std::string value = dbService.GetEntry(key);
  return value;
}

bool DatabaseClient::DeleteEntry(const std::string &key) {
  bool status = dbService.DeleteEntry(key);
  return status;
}

void DatabaseClient::populateInitialKVStore(
    std::map<std::string, std::string> &store, const std::string &startKey,
    const std::string &endKey) {

  std::map<int, std::vector<std::string>> response =
      dbService.GetAllKeysByRange(startKey, endKey);
  store.clear();
  for (const auto &entry : response) {
    int64_t timestamp = entry.first;
    std::string command = entry.second[0];
    std::string key = entry.second[1];
    if (command == "PUT" || command == "SWAP") {
      std::string value = entry.second[2];
      store[key] = value;
    } else if (command == "DELETE") {
      if (store.find(key) != store.end())
        store.erase(key);
    }

    // std::cout << "Entry Found - Timestamp: " << timestamp
    //             << ", Command: " << command
    //             << ", Key: " << key
    //             <<  "\n";
  }
}
