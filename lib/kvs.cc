#include "cloudlab/kvs.hh"

#include "rocksdb/db.h"

#include <iostream>
using namespace std;

namespace cloudlab {

auto KVS::open() -> bool {
  if (!db) {
    rocksdb::Options options;
    options.create_if_missing = true;
    return rocksdb::DB::Open(options, path.string(), &db).ok();
  }

  return true;
}

auto KVS::get(const std::string& key, std::string& result) -> bool {
  std::lock_guard<std::mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Get(rocksdb::ReadOptions(), key, &result).ok();
}

auto KVS::get_all(std::vector<std::pair<std::string, std::string>>& buffer)
    -> bool {
  std::lock_guard<std::mutex> lck(mtx);
  if (!kvs_open){ kvs_open = open(); }
  auto* it = db->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();

  while (it->Valid()) {
    string key  (it->key  ().data(), it->key  ().size());
    string value(it->value().data(), it->value().size());
    
    buffer.emplace_back(key, value);
    it->Next();
  }

  return true;
}

auto KVS::put(const std::string& key, const std::string& value) -> bool {
  std::lock_guard<std::mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Put(rocksdb::WriteOptions(), key, value).ok();
}

auto KVS::remove(const std::string& key) -> bool {
  std::lock_guard<std::mutex> lck(mtx);
  if (!kvs_open) kvs_open = open();
  return db && db->Delete(rocksdb::WriteOptions(), key).ok();
}

auto KVS::clear() -> bool {
  std::lock_guard<std::mutex> lck(mtx);
  return rocksdb::DestroyDB(path.string(), {}).ok();
}

auto KVS::clear_partition(size_t id) -> bool {
  // TODO(you)
  return {};
}

auto KVS::begin() -> KVS::Iterator {
  // TODO(you)
  return {};
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
auto KVS::end() const -> KVS::Sentinel {
  return {};
}

auto KVS::Iterator::operator*()
    -> std::pair<std::string_view, std::string_view> {
  // TODO(you)
  return {};
}

auto KVS::Iterator::operator++() -> KVS::Iterator& {
  // TODO(you)

  return *this;
}

auto operator==(const KVS::Iterator& it, const KVS::Sentinel&) -> bool {
  // TODO(you)
  return {};
}

auto operator!=(const KVS::Iterator& lhs, const KVS::Sentinel& rhs) -> bool {
  return !(lhs == rhs);
}

KVS::~KVS() {
  // TODO(you)
}

KVS::Iterator::~Iterator() {
  // TODO(you)
}

auto KVS::Partition::begin() const -> KVS::Iterator {
  // TODO(you)
  return {};
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
auto KVS::Partition::end() const -> KVS::Sentinel {
  return {};
}

}  // namespace cloudlab