#include "cloudlab/handler/api.hh"
#include "cloudlab/handler/p2p.hh"
#include "cloudlab/network/server.hh"
#include "cloudlab/raft/raft.hh"
#include "argh.hh"
#include <fmt/core.h>

using namespace cloudlab;

auto main(int argc, char* argv[]) -> int {
  srand(time(nullptr) ^ getpid());

  argh::parser cmdl({"-a", "--api", "-p", "--p2p"});
  cmdl.parse(argc, argv);

  std::string api_address, p2p_address, clust_address;
  cmdl({"-a", "--api"}, "127.0.0.1:31000") >> api_address;
  cmdl({"-p", "--p2p"}, "127.0.0.1:32000") >> p2p_address;
  cmdl({"-c", "--ca"}, "127.0.0.1:41000") >> clust_address;

  if (cmdl[{"-l", "--leader"}]) {
    auto routing = Routing(clust_address);

    auto api_handler = APIHandler(routing);
    auto api_server = Server(api_address, api_handler);
    auto api_thread = api_server.run();

    auto p2p_handler = P2PHandler(routing);
    auto p2p_server = Server(clust_address, p2p_handler);
    p2p_handler.set_raft_leader();
    auto p2p_thread = p2p_server.run();
    auto raft_thread = p2p_handler.raft_run();

    fmt::print("leader up and running ...\n");

    api_thread.join();
    p2p_thread.join();
    raft_thread.join();
  }
  else {
    auto routing = Routing(p2p_address);

    // cluster address is the router address
    routing.set_cluster_address(SocketAddress{clust_address});

    auto api_handler = APIHandler(routing);
    auto api_server = Server(api_address, api_handler);
    auto api_thread = api_server.run();

    auto p2p_handler = P2PHandler(routing);
    auto p2p_server = Server(p2p_address, p2p_handler);
    p2p_handler.set_raft_follower();
    auto p2p_thread = p2p_server.run();
    auto raft_thread = p2p_handler.raft_run();

    fmt::print("KVS up and running ...\n");

    api_thread.join();
    p2p_thread.join();
    raft_thread.join();
  }
}