#include "dbserviceimpl.h"
#include <iostream>
#include <map>
#include <memory>

class DatabaseClient {
private:
  DatabaseServiceImpl dbService;

public:
  DatabaseClient() {}

  DatabaseClient(std::string db_path)
      : dbService(db_path) { // Directly initialize dbService
  }

  bool InsertEntry(int64_t timestamp, const std::string &command,
                   const std::string &key, const std::string &value);
  std::string GetEntry(const std::string &key);
  bool DeleteEntry(const std::string &key);
  void populateInitialKVStore(std::map<std::string, std::string> &store,
                              const std::string &startKey,
                              const std::string &endKey);
};