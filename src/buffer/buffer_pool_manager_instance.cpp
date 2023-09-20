//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <cstddef>
#include <cstdlib>

#include "buffer/lru_k_replacer.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/index/index.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (replacer_->Evict(&frame_id)) {  // pincount=0时才能被置换
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      page_table_->Remove(pages_[frame_id].GetPageId());  // 维护哈希表
    } else {
      return nullptr;
    }
  } else {
    frame_id = free_list_.back();
    free_list_.pop_back();  // 维护free_list_
  }
  page_id_t id = AllocatePage();
  *page_id = id;
  Page *new_page = pages_ + frame_id;
  // metadata:
  new_page->ResetMemory();
  new_page->page_id_ = id;
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;

  page_table_->Insert(id, frame_id);         // 维护哈希表
  replacer_->RecordAccess(frame_id);         // 维护LRU-K
  replacer_->SetEvictable(frame_id, false);  // pin
  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t value = -1;
  // 在内存中找到该页
  if (page_table_->Find(page_id, value)) {
    (pages_ + value)->pin_count_++;
    replacer_->RecordAccess(value);
    replacer_->SetEvictable(value, false);
    return pages_ + value;
  }
  // 内存没有，需从磁盘读取page到内存
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (replacer_->Evict(&frame_id)) {
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
      page_table_->Remove(pages_[frame_id].GetPageId());  // 维护哈希表
    } else {
      return nullptr;
    }
  } else {
    frame_id = free_list_.back();
    free_list_.pop_back();  // 维护free_list_
  }
  Page *new_page = pages_ + frame_id;
  // metadata:
  new_page->ResetMemory();
  new_page->page_id_ = page_id;
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());

  page_table_->Insert(page_id, frame_id);    // 维护哈希表
  replacer_->RecordAccess(frame_id);         // 维护LRU-K
  replacer_->SetEvictable(frame_id, false);  // pin

  return new_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t index = -1;
  // 在内存中找不到该页
  if (!page_table_->Find(page_id, index)) {
    return false;
  }
  if (pages_[index].pin_count_ <= 0) {
    return false;
  }
  pages_[index].pin_count_--;
  if (pages_[index].pin_count_ == 0) {
    replacer_->SetEvictable(index, true);
  }
  if (is_dirty) {
    pages_[index].is_dirty_ = true;
  }
  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {  // 判断pin_count才可以flush
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t value = -1;
  // 在内存中找到该页
  if (!page_table_->Find(page_id, value)) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[value].GetData());
  pages_[value].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  for (size_t i = 0; i < pool_size_; i++) {
    disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
    pages_[i].is_dirty_ = false;
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t value = -1;
  // 在内存中找到该页
  if (!page_table_->Find(page_id, value)) {
    return true;
  }
  if (pages_[value].pin_count_ != 0) {
    return false;
  }
  page_table_->Remove(page_id);    // 维护哈希表
  replacer_->Remove(value);        // 维护replacer
  free_list_.emplace_back(value);  // 维护free_list_
  pages_[value].ResetMemory();
  pages_[value].is_dirty_ = false;
  pages_[value].pin_count_ = 0;
  pages_[value].page_id_ = INVALID_PAGE_ID;
  // 这里需要清除其他标志位么？
  DeallocatePage(page_id);  // 这干啥的？没有具体实现 可以用吗？
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
