#include "dbserviceimpl.h"
#include <iostream>
#include <map>
#include <memory>
#include <sqlite3.h>
#include <string>
#include <vector>
bool DatabaseServiceImpl::InsertEntry(int64_t timestamp1,
                                      const std::string &command1,
                                      const std::string &key1,
                                      const std::string &value1) {
  // std::cout << timestamp1 << " " << command1 << " " << key1 << " " << value1;
  const char *sql = "INSERT INTO state_machine_commands (timestamp, command, "
                    "key, value) VALUES (?, ?, ?, ?);";
  sqlite3_stmt *stmt;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    std::cout << "Error message found for INSERT entry with key:" << key1
              << " cmd:" << command1 << " val:" << value1
              << " ts:" << timestamp1 << std::string(sqlite3_errmsg(db))
              << "\n";
    return false;
  }

  sqlite3_bind_int64(stmt, 1, timestamp1);
  sqlite3_bind_text(stmt, 2, command1.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 3, key1.c_str(), -1, SQLITE_STATIC);

  sqlite3_bind_text(stmt, 4, value1.c_str(), -1, SQLITE_STATIC);

  //    sqlite3_bind_null(stmt, 4);

  if (sqlite3_step(stmt) == SQLITE_DONE) {
    // std::cout << "Entry inserted successfully.";
    sqlite3_finalize(stmt);
  } else {
    std::cout << "Insertion failed: for key" << key1
              << std::string(sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    return false;
  }
  return true;
}

std::string DatabaseServiceImpl::GetEntry(const std::string key) {
  const char *sql = "SELECT timestamp, command, value FROM "
                    "state_machine_commands WHERE key = ?;";
  sqlite3_stmt *stmt;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    std::cout << "Error message found for GET entry with key:" << key
              << std::string(sqlite3_errmsg(db)) << "\n";
    return "";
  }

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);
  std::string res;
  if (sqlite3_step(stmt) == SQLITE_ROW) {
    if (sqlite3_column_type(stmt, 2) != SQLITE_NULL) {
      res = reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2));
      sqlite3_finalize(stmt);
    }
  } else {
    std::cout << "Error in returning GET operation result for key" << key
              << std::string(sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    return "";
  }

  return res;
}

bool DatabaseServiceImpl::DeleteEntry(const std::string key) {
  const char *sql = "DELETE FROM state_machine_commands WHERE key = ?;";
  sqlite3_stmt *stmt;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    std::cout << "Error message found for DELETE entry with key:" << key
              << std::string(sqlite3_errmsg(db)) << "\n";
    return false;
  }

  sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_STATIC);

  if (sqlite3_step(stmt) == SQLITE_DONE) {
    sqlite3_finalize(stmt);
  } else {
    std::cout << "Deletion failed: " << std::string(sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    return false;
  }

  return true;
}

std::map<int, std::vector<std::string>>
DatabaseServiceImpl::GetAllKeysByRange(std::string startKey,
                                       std::string endKey) {
  const char *sql =
      "SELECT timestamp, command, key, value FROM state_machine_commands WHERE "
      "key BETWEEN ? AND ? ORDER BY timestamp;";
  sqlite3_stmt *stmt;

  if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
    std::cout
        << "Error message found for GETALLKEYSBYRANGE entry with start key:"
        << startKey << std::string(sqlite3_errmsg(db)) << "\n";
    return {};
  }

  // Bind start_key and end_key
  sqlite3_bind_text(stmt, 1, startKey.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, endKey.c_str(), -1, SQLITE_STATIC);
  std::map<int, std::vector<std::string>> response;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    std::vector<std::string> res;
    res.push_back(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 1)));
    res.push_back(reinterpret_cast<const char *>(sqlite3_column_text(stmt, 2)));

    if (sqlite3_column_type(stmt, 2) != SQLITE_NULL) {
      res.push_back(
          reinterpret_cast<const char *>(sqlite3_column_text(stmt, 3)));
    }

    response[sqlite3_column_int64(stmt, 0)] = res;
  }

  sqlite3_finalize(stmt);
  return response;
}

// void RunServer(const std::string& db_path, const std::string& server_address)
// {
//     DatabaseServiceImpl service(db_path);
//     service.GetEntryWithoutGRPC("srihari");
// }

// int main(int argc, char** argv) {
//     std::string db_path = "data.db";
//     if (argc > 1) {
//         db_path = argv[1];
//     }

//     RunServer(db_path, server_address);
//     return 0;
// }
