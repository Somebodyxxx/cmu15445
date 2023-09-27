//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return array_[index].second;
}

/* ================ my function===============*/
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const KeyType& key,const KeyComparator& comparator_) -> int{
  //找到第一个>=key的位置->left,注意：return的是left-1 即前一个位置
//   int left = 1;
//   int right = this->GetSize()-1;
//   if(comparator_(this->array_[right].first,key) >= 0){
//     return right;
//   }
//   while(left <= right){
//     int mid = left + (right - left)/2;
//     if(comparator_(this->KeyAt(mid),key) < 0){
//       left = mid+1;//没有问题，当right=left+1时，mid=left
//     }else{
//       right = mid-1;
//     }
//   }
//   // 指针的Index对应于第一个>=key的位置的前一个key-value
//   return left-1;
// }
  //找到第一个比key大的存key的位置
  int i = 1;
  for(;i < GetSize();i++){
    if(comparator_(array_[i].first,key)>0){
      break;
    }
  }
  return i;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType& key,const ValueType& value,const KeyComparator& comparator_){
  int index = FindIndex(key, comparator_);
  int size = GetSize();
  for(int i = size; i > index;i--){
    array_[i] = array_[i-1];
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAfter(const KeyType& key,const ValueType& value){
  int last = GetSize();
  array_[last] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValue0(const ValueType& value){
  if(GetSize()==0){
    IncreaseSize(1);
  }
  array_[0].second = value;
}


// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
