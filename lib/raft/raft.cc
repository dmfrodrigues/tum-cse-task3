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
    message.mutable_address()->set_address(own_addr);
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
    message.mutable_address()->set_address(own_addr);
    message.set_message(own_addr);
    con.send(message);
    con.receive(res);

    for(const string &p: peers){
      message.set_message(p);
      con.send(message);
      con.receive(res);
    }
  }

  cerr << "[Raft] Inserting " << peer.string() << " to peers set" << endl;
  peers.insert(peer.string());
}

auto Raft::put(const std::string& key, const std::string& value) -> bool {
  cloud::CloudMessage msg;
  msg.set_type(cloud::CloudMessage_Type_REQUEST);
  msg.set_operation(cloud::CloudMessage_Operation_PUT);
  auto tmp = msg.add_kvp();
  tmp->set_key(key);
  tmp->set_value(value);
  for(const string &peer: peers){
    Connection con{peer};
    con.send(msg);
    cloud::CloudMessage response;
    con.receive(response);
  }
  return true;
}

auto Raft::perform_election(Routing& routing) -> void {
  set_candidate();
  leader_addr = "";

  const size_t numberNodes = peers.size() + 1;
  const size_t votesThreshold = numberNodes/2 + 1;
  
  while(true){
    if(leader_addr != ""){
      cerr << "[Candidate] New leader that not me elected, stepping down" << endl;
      set_follower();
      return;
    }

    current_term++;
    voted_for = SocketAddress{own_addr};

    votes_received = 1;

    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_VOTE);
    msg.set_message(to_string(current_term) + " " + own_addr);

    cloud::CloudMessage response{};

    for(const string &peer: peers){
      if(votes_received >= votesThreshold)
        break;

      Connection con{peer};
      con.send(msg);
      con.receive(response);
      if(response.message() == own_addr){
        ++votes_received;
        cerr << "[Candidate] term=" << current_term << ", got 1 more vote" << endl;
      }
    }
    if(votes_received >= votesThreshold){
      set_leader();
      leader_addr = own_addr;
      cerr << "[Leader] New leader elected" << endl;
      return;
    } else {
      cerr << "[Candidate] Election failed" << endl;
    }
  }
}

auto Raft::vote(uint64_t term, SocketAddress candidate) -> optional<SocketAddress> {
  if(term < current_term){
    return nullopt;
  } else if(term == current_term){
    if(voted_for == nullopt){
      voted_for = candidate;
      cerr << "[Follower] term=" << term << ", voted for " << candidate.string() << endl;
    }
    return voted_for;
  } else { // term > current_term
    reset_election_timer(candidate.string());
    current_term = term;
    cerr << "[Follower] term=" << term << ", voted for " << candidate.string() << endl;
    return voted_for = candidate;
  }
}

auto Raft::heartbeat(Routing& routing, std::mutex& mtx) -> void {
  cerr << "[Heartbeat] Broadcasting" << endl;
  for(auto it = peers.begin(); it != peers.end();){
    const string peer = *it;

    cloud::CloudMessage msg{};
    msg.set_type(cloud::CloudMessage_Type_REQUEST);
    msg.set_operation(cloud::CloudMessage_Operation_RAFT_APPEND_ENTRIES);
    auto addr = msg.mutable_address();
    addr->set_address(own_addr);
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
          lock_guard<mutex> lock(mtx);
          election_timer_local = election_timer;
        }
        if(
          peers.size() > 0 &&
          election_timer_local < std::chrono::high_resolution_clock::now()
        ){
          cerr << "[Candidate] Leader suspected to be dead, starting election" << endl;
          remove_node(leader_addr);
          perform_election(routing);
        }
      }
    }
  });
}


}  // namespace cloudlab