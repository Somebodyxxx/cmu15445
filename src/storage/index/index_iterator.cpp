/**
 * index_iterator.cpp
 */
#include <cassert>
#include <stdexcept>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_node, int index,
                                  BufferPoolManager *buffer_pool_manager)
    : leaf_node_(leaf_node), key_index_(index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  int leaf_size = leaf_node_->GetSize();
  return key_index_ >= leaf_size && leaf_node_->GetNextPageId() == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_node_->MappingAt(key_index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  int leaf_size = leaf_node_->GetSize();
  if (key_index_ >= leaf_size) {
    throw "B+ tree index_iterator '++' out of range";
    assert(0);
  }
  if (key_index_ < leaf_size - 1) {
    key_index_++;
    return *this;
  }
  page_id_t next_page_id = leaf_node_->GetNextPageId();
  if (next_page_id == -1) {
    key_index_++;
    return *this;
  }
  Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
  auto next_leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
  leaf_node_ = next_leaf;
  key_index_ = 0;
  buffer_pool_manager_->UnpinPage(next_page_id, false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
