#include "cloudlab/handler/p2p.hh"

#include "fmt/core.h"

#include "cloud.pb.h"

#include <iostream>

using namespace std;

namespace cloudlab {

P2PHandler::P2PHandler(Routing& routing) : routing{routing} {
  auto hash = std::hash<SocketAddress>()(routing.get_backend_address());
  auto path = fmt::format("/tmp/{}-initial", hash);
  auto raft_path = fmt::format("/tmp/{}-raft", hash);
  partitions.insert({0, std::make_unique<KVS>(path)});
  raft = std::make_unique<Raft>(raft_path, routing.get_backend_address().string());
}

auto P2PHandler::handle_connection(Connection& con) -> void {
  cloud::CloudMessage request{}, response{};

  if (!con.receive(request)) {
    return;
  }

  switch (request.operation()) {
    case cloud::CloudMessage_Operation_PUT: {
      if (raft->leader()) {
        handle_key_operation_leader(con, request);
      }
      else {
        handle_put(con, request);
      }
      break;
    }
    case cloud::CloudMessage_Operation_GET: {
      if (raft->leader()) {
        handle_key_operation_leader(con, request);
      }
      else {
        handle_get(con, request);
      }
      break;
    }
    case cloud::CloudMessage_Operation_DELETE: {
      if (raft->leader()) {
        handle_key_operation_leader(con, request);
      }
      else {
        handle_delete(con, request);
      }
      break;
    }
    case cloud::CloudMessage_Operation_JOIN_CLUSTER: {
      handle_join_cluster(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_CREATE_PARTITIONS: {
      handle_create_partitions(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_STEAL_PARTITIONS: {
      handle_steal_partitions(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_DROP_PARTITIONS: {
      handle_drop_partitions(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_TRANSFER_PARTITION: {
      handle_transfer_partition(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES: {
      handle_raft_append_entries(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_VOTE: {
      handle_raft_vote(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_DROPPED_NODE: {
      handle_raft_dropped_node(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_GET_LEADER: {
      handle_raft_get_leader(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_DIRECT_GET: {
      handle_raft_direct_get(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_ADD_NODE: {
      handle_raft_add_node(con, request);
      break;
    }
    case cloud::CloudMessage_Operation_RAFT_REMOVE_NODE: {
      handle_raft_remove_node(con, request);
      break;
    }

    default:
      response.set_type(cloud::CloudMessage_Type_RESPONSE);
      response.set_operation(request.operation());
      response.set_success(false);
      response.set_message("Operation not (yet) supported");

      con.send(response);

      break;
  }
}

auto P2PHandler::handle_put(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition_id = routing.get_partition(kvp.key());

    if (partitions.contains(partition_id) &&
        partitions.at(partition_id)->put(kvp.key(), kvp.value())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
      response.set_success(false);
      response.set_message("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_get(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};
  std::string value;

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition_id = routing.get_partition(kvp.key());

    if (partitions.contains(partition_id) &&
        partitions.at(partition_id)->get(kvp.key(), value)) {
      tmp->set_value(value);
    } else {
      tmp->set_value("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_delete(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_success(true);
  response.set_message("OK");

  for (const auto& kvp : msg.kvp()) {
    auto* tmp = response.add_kvp();
    tmp->set_key(kvp.key());

    auto partition = routing.get_partition(kvp.key());

    if (partitions.contains(partition) &&
        partitions.at(partition)->remove(kvp.key())) {
      tmp->set_value("OK");
    } else {
      tmp->set_value("ERROR");
    }
  }

  con.send(response);
}

auto P2PHandler::handle_key_operation_leader(Connection& con, const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  switch(msg.operation()){
    case cloud::CloudMessage_Operation_GET:
      handle_get(con, msg);
      return;
    case cloud::CloudMessage_Operation_PUT:
      for (const auto& kvp : msg.kvp()) {
        auto* tmp = response.add_kvp();
        tmp->set_key(kvp.key());
        tmp->set_value("OK");

        auto partition_id = routing.get_partition(kvp.key());
        assert(partitions.at(partition_id)->open());
        assert(partitions.at(partition_id)->put(kvp.key(), kvp.value()));

        assert(raft->put(kvp.key(), kvp.value()));
      }
      response.set_message("OK");
      con.send(response);
      return;
    case cloud::CloudMessage_Operation_DELETE:
      break;
  }
}

auto P2PHandler::handle_join_cluster(Connection& con,
                                     const cloud::CloudMessage& msg) -> void {
  cloud::CloudMessage response{};

  // TODO(you)
  // Handle join cluster request. Leader might operate differently from followers.
  
  auto peer = SocketAddress(msg.address().address());
  raft->join_peer(peer);

  cout << "[Join] Added " << msg.address().address() << endl;

  // Give new peer all the KVS
  {
    Connection connectionToPeer{peer};

    cloud::CloudMessage message;
    message.set_type(cloud::CloudMessage_Type_REQUEST);
    message.set_operation(cloud::CloudMessage_Operation_PUT);
    for(const auto &p: partitions){
      const auto &partition = p.second;
      vector<pair<string,string>> buffer;
      partition->get_all(buffer);
      for(const auto &kvp: buffer){
        auto *tmp = message.add_kvp();
        tmp->set_key(kvp.first);
        tmp->set_value(kvp.second);
      }
    }

    connectionToPeer.send(message);
    cloud::CloudMessage response;
    connectionToPeer.receive(response);
    
  }

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_JOIN_CLUSTER);
  response.set_message("OK");

  con.send(response);
}

auto P2PHandler::handle_create_partitions(Connection& con,
                                          const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  // TODO from the 2nd task - not required

  con.send(response);
}

auto P2PHandler::handle_steal_partitions(Connection& con,
                                         const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  // TODO from the 2nd task - not required

  con.send(response);
}

auto P2PHandler::handle_drop_partitions(Connection& con,
                                        const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  // TODO from the 2nd task - not required

  con.send(response);
}

auto P2PHandler::handle_transfer_partition(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  // TODO from the 2nd task - not required

  con.send(response);
}

auto P2PHandler::handle_raft_append_entries(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  raft->reset_election_timer(msg.address().address());

  cloud::CloudMessage response{};

  con.send(response);
}

auto P2PHandler::handle_raft_vote(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};
  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);
  
  size_t idx = msg.message().find(" ");
  string termStr = msg.message().substr(0, idx);
  string candidateStr = msg.message().substr(idx+1);

  uint64_t term = atoll(termStr.c_str());
  SocketAddress candidate{candidateStr};

  optional<SocketAddress> castedVote = raft->vote(term, candidate);
  if(castedVote == nullopt){
    response.set_message("ERROR");
    con.send(response);
    return;
  }
  response.set_message(castedVote.value().string());
  con.send(response);
}

auto P2PHandler::handle_raft_dropped_node(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {

  cerr << "[Dropped] Starting to process" << endl;

  cloud::CloudMessage response{};

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_DROPPED_NODE);
  vector<string> droppedPeers;
  raft->get_dropped_peers(droppedPeers);
  stringstream ss;
  for(const string &peerAddress: droppedPeers){
    ss << peerAddress << "\n";
  }
  response.set_message(ss.str());

  con.send(response);
}

auto P2PHandler::handle_raft_get_leader(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  response.set_type(cloud::CloudMessage_Type_RESPONSE);
  response.set_operation(cloud::CloudMessage_Operation_RAFT_GET_LEADER);

  string leaderAddr;
  raft->get_leader_addr(leaderAddr);
  response.set_message(leaderAddr);

  con.send(response);
}

auto P2PHandler::handle_raft_direct_get(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  handle_get(con, msg);

}

auto P2PHandler::handle_raft_add_node(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  raft->reset_election_timer(msg.address().address());

  raft->add_node(msg.message());
  cerr << "[Node] Added node " << msg.message() << endl;

  response.set_message("OK");

  con.send(response);
}

auto P2PHandler::handle_raft_remove_node(Connection& con,
                                           const cloud::CloudMessage& msg)
    -> void {
  cloud::CloudMessage response{};

  raft->remove_node(msg.message());

  response.set_message("OK");

  con.send(response);
}



}  // namespace cloudlab