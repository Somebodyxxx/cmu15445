//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <sstream>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

/* ================= my function =================*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MappingAt(int index) -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindKey(const KeyType &key, ValueType &value, const KeyComparator &comparator_)
    -> bool {
  // int index = FindIndex(key,comparator_);
  // if(comparator_(array_[index].first,key)==0){
  //   value = array_[index].second;
  //   return true;
  // }
  // return false;

  // int left = 0;
  // int right = this->GetSize()-1;
  // while(left <= right){
  //   int mid = left + (right - left)/2;
  //   if(comparator_(this->KeyAt(mid),key) > 0){
  //     right = mid-1;
  //   }else if(comparator_(this->KeyAt(mid),key) < 0){
  //     left = mid+1;
  //   }else{
  //     value = array_[mid].second;
  //     return true;
  //   }
  // }
  // return false;
  // assert(GetSize() <= GetMaxSize());
  for (int i = 0; i < GetSize(); i++) {
    if (comparator_(array_[i].first, key) == 0) {
      value = array_[i].second;
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindIndex(const KeyType &key, const KeyComparator &comparator_) -> int {
  // //找到第一个>=key的位置
  // int left = 0;
  // int right = this->GetSize()-1;
  // while(left <= right){
  //   int mid = left + (right - left)/2;
  //   if(comparator_(this->KeyAt(mid),key) < 0){
  //     left = mid+1;//没有问题，当right=left+1时，mid=left
  //   }else{
  //     right = mid-1;
  //   }
  // }
  // // 指针的Index对应于第一个>=key的位置的前一个key-value
  // return left;
  int i = 0;
  for (; i < GetSize(); i++) {
    if (comparator_(array_[i].first, key) >= 0) {
      break;
    }
  }
  return i;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertAfter(const KeyType &key, const ValueType &value) {
  int last = GetSize();
  array_[last] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator_) {
  int index = FindIndex(key, comparator_);
  int size = GetSize();
  for (int i = size; i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator_) {
  int index = FindIndex(key, comparator_);
  if (comparator_(array_[index].first, key) != 0) {
    // 没找对，不存在该key，不能删
    // LOG_DEBUG("no key to remove in this leafnode , return...");
    return;
  }
  int size = GetSize();

  // 如果没有这句if，index找到了size处，for虽然不会进去，最后size--会执行。
  if (index >= size) {
    // 没找对，不存在该key，不能删
    // LOG_DEBUG("no key to remove in this leafnode , return...");
    return;
  }

  for (int i = index; i + 1 < size; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopBack() -> MappingType {
  int size = GetSize();
  MappingType kv = std::make_pair(array_[size - 1].first, array_[size - 1].second);
  IncreaseSize(-1);
  return kv;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(MappingType kv) {
  int old_size = GetSize();
  IncreaseSize(1);
  for (int i = old_size; i > 0; i--) {
    array_[i] = array_[i - 1];
  }
  array_[0] = kv;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PopFront() -> MappingType {
  MappingType kv = std::make_pair(array_[0].first, array_[0].second);
  int size = GetSize();
  for (int i = 0; i + 1 < size; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return kv;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(MappingType kv) {
  array_[GetSize()] = kv;
  IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
