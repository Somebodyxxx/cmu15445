//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <algorithm>
#include <cstddef>
#include <mutex>    // NOLINT
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : curr_size_(0), replacer_size_(num_frames), k_(k), fifo_size_(0), lru_size_(0) {
  l_ = new LinkedNode(-1);
  r_ = new LinkedNode(-1);
  m_ = new LinkedNode(-1);
  l_->right_ = m_;
  m_->left_ = l_;
  m_->right_ = r_;
  r_->left_ = m_;
}
LRUKReplacer::~LRUKReplacer() {
  auto p = l_;
  LinkedNode *next = nullptr;
  while (p != nullptr) {
    next = p->right_;
    delete p;
    p = next;
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }
  LinkedNode *node = m_->left_;
  while (node != m_) {
    if (node == l_) {
      node = r_->left_;
      continue;
    }
    if (node->evictable_) {
      node->left_->right_ = node->right_;
      node->right_->left_ = node->left_;
      *frame_id = node->key_;
      if (node->frequency_ >= k_) {
        lru_size_--;
      } else {
        fifo_size_--;
      }
      map_.erase(*frame_id);
      delete node;
      return true;
    }
    node = node->left_;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "invalid frame_id in LRU_K RecordAccess..";
  }
  if (map_.count(frame_id) == 1) {
    map_[frame_id]->frequency_++;
    if (map_[frame_id]->frequency_ >= k_) {  // 移到lru链表头
      map_[frame_id]->left_->right_ = map_[frame_id]->right_;
      map_[frame_id]->right_->left_ = map_[frame_id]->left_;
      map_[frame_id]->left_ = m_;
      map_[frame_id]->right_ = m_->right_;
      m_->right_->left_ = map_[frame_id];
      m_->right_ = map_[frame_id];
    }
  } else {
    // if(curr_size_ == replacer_size_){
    //     frame_id_t *frame_id = nullptr;
    //     Evict(frame_id);
    // }
    auto node = new LinkedNode(frame_id);
    curr_size_++;
    node->left_ = l_;
    node->right_ = l_->right_;
    l_->right_->left_ = node;
    l_->right_ = node;

    map_[frame_id] = node;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) > replacer_size_) {
    throw "invalid frame_id in LRU_K RecordAccess..";
  }
  if (map_.count(frame_id) == 0) {
    return;
  }
  bool old_flag = map_[frame_id]->evictable_;
  if ((set_evictable && old_flag) || (!set_evictable && !old_flag)) {
    // 没有改变
    return;
  }
  map_[frame_id]->evictable_ = set_evictable;
  int change = 1;
  if (old_flag) {
    change = -1;
  }
  if (map_[frame_id]->frequency_ >= 2) {
    lru_size_ += change;
  } else {
    fifo_size_ += change;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (map_.count(frame_id) == 0) {
    return;
  }
  LinkedNode *node = map_[frame_id];
  if (node->evictable_) {
    node->left_->right_ = node->right_;
    node->right_->left_ = node->left_;
    if (node->frequency_ >= 2) {
      lru_size_--;
    } else {
      fifo_size_--;
    }
    delete node;
    map_.erase(frame_id);
  }
}

auto LRUKReplacer::Size() -> size_t { return lru_size_ + fifo_size_; }

// ===============================LinkedNode==================================
LRUKReplacer::LinkedNode::LinkedNode(frame_id_t key)
    : key_(key), frequency_(1), left_(nullptr), right_(nullptr), evictable_(false) {}

}  // namespace bustub
