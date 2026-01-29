#include "../generated/clustermanagerservice.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <map>
#include <memory>
#include <sqlite3.h>
#include <string>
#include <utility>
#include <vector>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;

using keyvalue::ClientRegistrationResponse;
using keyvalue::ClusterManagerService;
using keyvalue::EmptyRequest;
using keyvalue::ServiceRegistrationRequest;
using keyvalue::ServiceRegistrationResponse;

class ClusterManager final : public ClusterManagerService::Service {
private:
  std::map<int, std::vector<std::pair<std::string, bool>>> ipAddresses;
  void printIPAddresses();
  std::vector<std::string> generateKeySpace();
  void parseServers(const std::string &servers, int server_replication_factor);

public:
  int serverReplicationFactor = 3;
  ClusterManager();
  ClusterManager(std::string servers, int server_replication_factor);
  virtual ~ClusterManager();

  Status RegisterServer(ServerContext *context,
                        const ServiceRegistrationRequest *request,
                        ServiceRegistrationResponse *reply);
  Status RegisterClient(ServerContext *context, const EmptyRequest *request,
                        ClientRegistrationResponse *reply);
};