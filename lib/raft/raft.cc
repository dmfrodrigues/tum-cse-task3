#include "cloudlab/raft/raft.hh"

#include <iostream>
using namespace std;

namespace cloudlab {

auto Raft::join_peer(const SocketAddress &peer) -> void {
  if(!leader()) throw logic_error("Only leader can join peers");
  
  // Inform nodes in the system of the entry of peer
  for(const SocketAddress &p: peers){
    Connection con{p};

    cloud::CloudMessage message;
    message.set_type(cloud::CloudMessage_Type_REQUEST);
    message.set_operation(cloud::CloudMessage_Operation_RAFT_ADD_NODE);
    message.set_message(peer.string());

    con.send(message);

    cloud::CloudMessage res;
    con.receive(res);
  }

  // Inform newly-added peer of the nodes already in the system
  {
    Connection con{peer};

    cloud::CloudMessage res;

    cloud::CloudMessage message;
    message.set_type(cloud::CloudMessage_Type_REQUEST);
    message.set_operation(cloud::CloudMessage_Operation_RAFT_ADD_NODE);
    message.set_message(own_addr);
    con.send(message);
    con.receive(res);

    for(const SocketAddress &p: peers){
      message.set_message(p.string());
      con.send(message);
      con.receive(res);
    }
  }

  peers.insert(peer);
}

auto Raft::put(const std::string& key, const std::string& value) -> bool {
  // TODO(you)
}

auto Raft::perform_election(Routing& routing) -> void {
  // TODO(you)
  // Upon election timeout, the follower changes to candidate and starts election  
}

auto Raft::heartbeat(Routing& routing, std::mutex& mtx) -> void {
  cerr << "[Heartbeat] Broadcasting" << endl;
  for(auto it = peers.begin(); it != peers.end();){
    const SocketAddress peer = *it;

    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    try {
      Connection con{peer};
      con.send(msg);
      con.receive(msg);
      cerr << "[Heartbeat] Sent to " << peer.string() << endl;
      ++it;
    } catch(const runtime_error &e){
      cerr << "[Heartbeat] "
        << "Runtime error: " << e.what() << "; "
        << "removing peer " << peer.string() << endl;
      dropped_peers.push_back(peer);
      it = peers.erase(it);

      for(const SocketAddress &p: peers){
        Connection con{p};

        cloud::CloudMessage message;
        message.set_type(cloud::CloudMessage_Type_REQUEST);
        message.set_operation(cloud::CloudMessage_Operation_RAFT_REMOVE_NODE);
        message.set_message(peer.string().c_str());

        con.send(message);

        cloud::CloudMessage res;
        con.receive(res);
      }
    }

  }
}

auto Raft::run(Routing& routing, std::mutex& mtx) -> std::thread {
  // TODO(you)
  // Return a thread that keeps running the heartbeat function.
  // If you have other implementation you can skip this.   

  if(leader())
    return thread([this, &routing, &mtx](){
      while(true){
        this_thread::sleep_for(chrono::milliseconds(500));
        heartbeat(routing, mtx);
      }
    });

  return {};
}


}  // namespace cloudlab