#ifndef CLOUDLAB_RAFT_HH
#define CLOUDLAB_RAFT_HH

#include "cloudlab/kvs.hh"
#include "cloudlab/network/address.hh"
#include "cloudlab/network/connection.hh"
#include "cloudlab/network/routing.hh"

#include "cloud.pb.h"

#include <optional>
#include <random>
#include <span>
#include <thread>
#include <unordered_map>
#include <fmt/core.h>
#include <ctime>
#include <iostream>
#include <chrono>
#include <thread>
#include <mutex>

namespace cloudlab {

struct LogEntry {
  uint64_t term_;
  cloud::CloudMessage cmd_;
};

struct PeerIndices {
  uint64_t next_index_;
  uint64_t match_index_;
};

enum class RaftRole {
  LEADER,
  CANDIDATE,
  FOLLOWER,
};

class Raft {
 private:
  int MIN_ELECTION_TIMEOUT_MS = 1000;
  int MAX_ELECTION_TIMEOUT_MS = 2000;

 public:
  explicit Raft(const std::string& path = {}, const std::string& addr = {}, bool open = false):
    kvs{path, open}, own_addr{addr},
    election_timeout_val(
      std::chrono::milliseconds(
        MIN_ELECTION_TIMEOUT_MS +
        std::rand() % (MAX_ELECTION_TIMEOUT_MS - MIN_ELECTION_TIMEOUT_MS)
      )
    )
  {
    std::cerr
      << "[Raft] Election timeout is "
      << std::chrono::duration_cast<std::chrono::milliseconds>(election_timeout_val).count() << std::endl;
  }

  auto run(Routing& routing, std::mutex& mtx) -> std::thread;

  auto open() -> bool {
    return kvs.open();
  }

  auto join_peer(const SocketAddress &peer) -> void;

  auto get(const std::string& key, std::string& result) -> bool {
    return kvs.get(key, result);
  }

  auto get_all(std::vector<std::pair<std::string, std::string>>& buffer) 
      -> bool {
    return kvs.get_all(buffer);
  }

  auto put(const std::string& key, const std::string& value) -> bool;

  auto remove(const std::string& key) -> bool {
    return kvs.remove(key);
  }

  auto leader() -> bool {
    return role == RaftRole::LEADER;
  }

  auto candidate() -> bool {
    return role == RaftRole::CANDIDATE;
  }

  auto follower() -> bool {
    return role == RaftRole::FOLLOWER;
  }

  auto set_leader() -> void {
    role = RaftRole::LEADER;
    leader_addr = own_addr;
  }

  auto set_candidate() -> void {
    role = RaftRole::CANDIDATE;
  }
  
  auto set_follower() -> void {
    role = RaftRole::FOLLOWER;
  }

  auto get_role() -> RaftRole {
    return role;
  }
  
  auto term() -> uint64_t {
    return current_term;
  }


  auto election_timeout() -> bool {
    std::lock_guard<std::mutex> lock(timer_mtx);
    auto now = std::chrono::high_resolution_clock::now();
    return (election_timer < now);
  }

  auto reset_election_timer(std::string leader) -> void {
    std::lock_guard<std::mutex> lock(timer_mtx);
    auto now = std::chrono::high_resolution_clock::now();
    election_timer = now + election_timeout_val;
    if(leader_addr != leader){
      if(leader_addr == own_addr){
        std::cerr << "[Leader] Dethroned by " << leader << std::endl;
        set_follower();
      }
      std::cerr << "[Follower] Updating leader to " << leader << std::endl;
      leader_addr = leader;
      peers.insert(leader);
    }
  }


  auto perform_election(Routing& routing) -> void;

  auto vote(uint64_t term, SocketAddress candidate) -> std::optional<SocketAddress>;

  auto heartbeat(Routing& routing, std::mutex& mtx) -> void;

  auto get_dropped_peers(std::vector<std::string>& result) -> void {
    result.clear();
    result.reserve(dropped_peers.size());
    for(const SocketAddress &peer: dropped_peers){
      result.push_back(peer.string());
    }
  }

  auto set_leader_addr(const std::string& addr) -> void {
    leader_addr = addr;
  }

  auto get_leader_addr(std::string& result) -> void {
    result = leader_addr;
  }

  auto add_node(const std::string &peer) -> void {
    peers.insert(peer);
  }

  auto remove_node(const std::string &peer) -> void {
    peers.erase(peer);
  }

 private:
  auto worker(Routing& routing) -> void;
  // the actual kvs
  KVS kvs;

  std::unordered_set<std::string> peers;

  // every peer is initially a follower
  RaftRole role{RaftRole::FOLLOWER};

  std::string own_addr{""};
  std::string leader_addr{""};


  // persistent state on all servers
  uint64_t current_term{};
  std::optional<SocketAddress> voted_for{};

  // for returning dropped followers
  std::vector<SocketAddress> dropped_peers;

  std::atomic_uint16_t votes_received{0};
  // election timer
  std::mutex timer_mtx;
  std::chrono::high_resolution_clock::time_point election_timer;
  std::chrono::high_resolution_clock::duration election_timeout_val{};
};

}  // namespace cloudlab

#endif  // CLOUDLAB_RAFT_HH