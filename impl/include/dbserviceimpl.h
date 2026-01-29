#include <iostream>
#include <map>
#include <memory>
#include <sqlite3.h>
#include <vector>

class DatabaseServiceImpl {
public:
  sqlite3 *db;

  DatabaseServiceImpl() {}
  explicit DatabaseServiceImpl(const std::string db_path) {
    if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
      std::cerr << "Error opening database: " << sqlite3_errmsg(db)
                << std::endl;
      exit(1);
    }
    const char *sql = R"(
            CREATE TABLE IF NOT EXISTS state_machine_commands (
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT
            );
        )";
    char *errMsg = nullptr;
    if (sqlite3_exec(db, sql, nullptr, nullptr, &errMsg) != SQLITE_OK) {
      std::cerr << "Error creating table: " << errMsg << std::endl;
      sqlite3_free(errMsg);
    }
    // std::cout << "Database initialized successfully." << std::endl;
  }

  bool InsertEntry(int64_t timestamp, const std::string &command,
                   const std::string &key, const std::string &value);
  std::string GetEntry(const std::string key);
  bool DeleteEntry(const std::string key);
  std::map<int, std::vector<std::string>>
  GetAllKeysByRange(std::string startKey, std::string endKey);

  ~DatabaseServiceImpl() {
    std::cout << "in destructor of databse impl file";
    sqlite3_close(db);
  }

private:
  std::string db_path;
};