//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;
  }
  if (dir_[index] == nullptr) {
    return false;
  }
  if (!dir_[index]->Find(key, value)) {
    return false;
  }
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  if (index >= dir_.size()) {
    return false;
  }
  if (!dir_[index]->Remove(key)) {
    return false;
  }
  return true;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  while (true) {
    size_t index = IndexOf(key);
    auto cur_bucket = dir_[index];
    if (cur_bucket->Insert(key, value)) {
      return;
    }
    if (cur_bucket->IsFull()) {
      int cur_local_depth = GetLocalDepthInternal(index);
      if (global_depth_ == cur_local_depth) {
        // 目录扩容
        global_depth_++;
        size_t old_size = dir_.size();
        dir_.resize(old_size << 1);
        for (size_t i = 0; i < old_size; i++) {
          dir_[i + old_size] = dir_[i];
        }
      }
      // 桶分裂
      auto bucket1 = std::make_shared<Bucket>(bucket_size_, cur_local_depth + 1);
      auto bucket2 = std::make_shared<Bucket>(bucket_size_, cur_local_depth + 1);
      num_buckets_++;
      // 找到指向当前桶的index数量
      int cnt = 1 << (global_depth_ - cur_local_depth);
      // 找到最小的index
      size_t offset = index & ((1 << cur_local_depth) - 1);
      // 遍历所有的index,重新分配新桶到index
      size_t new_index = offset;
      for (int i = 0; i < cnt; i++) {
        new_index = (i << cur_local_depth) + offset;
        if ((i & 1) == 0) {
          dir_[new_index] = bucket1;
        } else {
          dir_[new_index] = bucket2;
        }
      }

      std::list<std::pair<K, V>> list_temp = cur_bucket->GetItems();
      for (auto it = list_temp.begin(); it != list_temp.end(); it++) {
        size_t temp_index = IndexOf(it->first);
        dir_[temp_index]->Insert(it->first, it->second);
      }
    } else {
      UNREACHABLE("insert error...");
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      value = it->second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
  }
  if (IsFull()) {
    return false;
  }
  list_.push_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
