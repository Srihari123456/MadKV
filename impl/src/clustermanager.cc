#include "clustermanager.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>

ClusterManager::ClusterManager() { std::cout << "In default constructor"; }
ClusterManager::ClusterManager(std::string servers,
                               int server_replication_factor) {
  std::cout << "Initializing servers table\n";
  serverReplicationFactor = server_replication_factor;
  parseServers(servers, server_replication_factor);
  printIPAddresses();
}

void ClusterManager::printIPAddresses() {
  for (const auto &entry : ipAddresses) {
    std::cout << "Cluster: " << entry.first << std::endl;
    for (const auto &ipInfo : entry.second) {
      std::cout << "\tIP: " << ipInfo.first << " | Registration Status: "
                << (ipInfo.second ? "Registered" : "Not Registered")
                << std::endl;
    }
  }
}

void ClusterManager::parseServers(const std::string &servers,
                                  int server_replication_factor) {
  std::stringstream ss(servers);
  std::string ip;
  int partitionId = 0;
  int count = 0;
  while (std::getline(ss, ip, ',')) {
    ipAddresses[partitionId].push_back({ip, false});
    count += 1;
    if (count >= server_replication_factor) {
      partitionId += 1;
      count = 0;
    }
  }
}

Status ClusterManager::RegisterServer(ServerContext *context,
                                      const ServiceRegistrationRequest *request,
                                      ServiceRegistrationResponse *reply) {
  auto it = ipAddresses.find(request->partitionid());
  if (it != ipAddresses.end()) {
    // Iterate through the vector of {IP, registration_status}
    for (auto &ipInfo : it->second) {
      if (ipInfo.first == request->ipaddress()) {
        ipInfo.second = true; // Update registration status
        std::cout << "Server with IP " << request->ipaddress()
                  << " is registered.\n";
        return Status::OK;
      }
    }
    std::cout << "IP " << request->ipaddress() << " not found in partition "
              << request->partitionid() << "\n";
  } else {
    std::cout << "Invalid Server IP " << request->ipaddress()
              << " (Partition ID not found)\n";
  }
  return Status::OK;
}

Status ClusterManager::RegisterClient(ServerContext *context,
                                      const EmptyRequest *request,
                                      ClientRegistrationResponse *reply) {
  // Iterate over partitions
  for (const auto &partition : ipAddresses) {
    const auto &partitionID = partition.first;
    const auto &servers =
        partition.second; // Vector of {IP, registration_status}

    bool foundRegistered = false;
    for (const auto &server : servers) {
      if (server.second) {                  // Check if registered
        reply->add_ipaddress(server.first); // Send the first registered IP
        foundRegistered = true;
      }
    }
    // If no server is registered for a partition, return UNAVAILABLE
    if (!foundRegistered) {
      return Status(grpc::StatusCode::UNAVAILABLE,
                    "Not all partitions have a registered server.");
    }
  }
  reply->set_serverreplicationfactor(serverReplicationFactor);
  return Status::OK;
}

ClusterManager::~ClusterManager() { std::cout << "Overriding default"; }

void RunClusterManager(std::string &man_port, std::string &servers,
                       int server_replication_factor) {
  ClusterManager clustermanagerservice(servers, server_replication_factor);
  ServerBuilder builder;
  std::string clustermanagerip = "0.0.0.0:" + man_port;
  builder.AddListeningPort(clustermanagerip, grpc::InsecureServerCredentials());
  builder.RegisterService(&clustermanagerservice);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Cluster Manager listening on " << clustermanagerip << std::endl;
  server->Wait();
}

int main(int argc, char **argv) {
  std::string man_port("3666");
  std::string servers("");
  int server_replication_factor = 3;
  if (argc > 1) {
    man_port = argv[1];
  }
  if (argc > 2) {
    servers = argv[2];
  }
  if (argc > 3) {
    server_replication_factor = std::stoi(argv[3]);
  }
  RunClusterManager(man_port, servers, server_replication_factor);
  return 0;
}
