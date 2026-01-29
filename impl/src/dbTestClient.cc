#include "dbClient.h"

int main(int argc, char **argv) {
  DatabaseClient client("data.db");

  // Insert an entry
  std::cout << "Inserting an entry...\n";
  client.InsertEntry(123456789, "PUT", "testKey", "value_");

  // Get a specific entry by key
  std::cout << "\nGetting entry with key 'testKey'...\n";
  client.GetEntry("testKey");

  // Delete an entry
  std::cout << "\nDeleting entry with key 'testKey'...\n";
  client.DeleteEntry("testKey");

  return 0;
}
