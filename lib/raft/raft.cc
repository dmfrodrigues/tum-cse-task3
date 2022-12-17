#include "cloudlab/raft/raft.hh"

#include <iostream>
using namespace std;

namespace cloudlab {

auto Raft::join_peer(const SocketAddress &peer) -> void {
  if(!leader()) throw logic_error("Only leader can join peers");
  
  // Inform nodes in the system of the entry of peer
  for(const string &p: peers){
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

    for(const string &p: peers){
      message.set_message(p);
      con.send(message);
      con.receive(res);
    }
  }

  peers.insert(peer.string());
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
    const string peer = *it;

    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    try {
      Connection con{peer};
      con.send(msg);
      con.receive(msg);
      cerr << "[Heartbeat] Sent to " << peer << endl;
      ++it;
    } catch(const runtime_error &e){
      cerr << "[Heartbeat] "
        << "Runtime error: " << e.what() << "; "
        << "removing peer " << peer << endl;
      dropped_peers.push_back(SocketAddress{peer});
      it = peers.erase(it);

      for(const string &p: peers){
        Connection con{p};

        cloud::CloudMessage message;
        message.set_type(cloud::CloudMessage_Type_REQUEST);
        message.set_operation(cloud::CloudMessage_Operation_RAFT_REMOVE_NODE);
        message.set_message(peer);

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

  return thread([this, &routing, &mtx](){
    while(true){
      if(leader()){
        this_thread::sleep_for(chrono::milliseconds(500));
        heartbeat(routing, mtx);
      } else if(follower()){
        std::chrono::high_resolution_clock::time_point election_timer_local;
        {
          lock_guard<mutex> lock(timer_mtx);
          election_timer_local = election_timer;
        }
        if(election_timer_local < std::chrono::high_resolution_clock::now()){
          peers.erase(leader_addr);
          perform_election(routing);
        }
      }
    }
  });
}


}  // namespace cloudlab