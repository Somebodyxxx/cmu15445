#include <cassert>
#include <cstring>
#include <string>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"
#include "type/value.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == -1; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  LeafPage *leafnode = FindLeaf(key);
  ValueType v;
  if (!leafnode->FindKey(key, v, comparator_)) {
    return false;
  }
  result->push_back(v);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    Page *new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_page);
    auto leaf_page = reinterpret_cast<LeafPage *>(new_page->GetData());
    leaf_page->Init(root_page_id_, -1, leaf_max_size_);
    leaf_page->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(1);
    return true;
  }
  // 判断重复
  std::vector<ValueType> result;
  if (GetValue(key, &result)) {
    return false;
  }
  InsertIntoLeaf(key, value);
  return true;
}

/* =========================my function =======================*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage * {
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto cur_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  while (!cur_node->IsLeafPage()) {
    auto inner_node = reinterpret_cast<InternalPage *>(cur_node);
    int index = inner_node->FindIndex(key, comparator_);  // 返回第一个比key大的位置
    auto page_id = inner_node->ValueAt(index - 1);
    Page *next_page = buffer_pool_manager_->FetchPage(page_id);
    cur_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  // cur_node is LeafPage
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return reinterpret_cast<LeafPage *>(cur_node);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value) {
  LeafPage *leaf_node = FindLeaf(key);
  leaf_node->Insert(key, value, comparator_);
  if (leaf_node->GetSize() < leaf_max_size_) {
    // 释放资源操作
    return;
  }
  // 分裂+重组
  auto new_leaf_node = LeafSplit(leaf_node);  //
  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node) {
  if (old_node->IsRootPage()) {
    auto new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto new_root = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root->Init(root_page_id_, -1, internal_max_size_);
    new_root->SetValue0(old_node->GetPageId());
    new_root->InsertAfter(key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(parent_page);
  auto parent_page_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_page_node->GetSize() < internal_max_size_) {
    parent_page_node->Insert(key, new_node->GetPageId(), comparator_);
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }

  auto mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_page_node->GetSize() + 1)];
  auto copy_parent_node = reinterpret_cast<InternalPage *>(mem);
  std::memcpy(mem, parent_page->GetData(),
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (parent_page_node->GetSize()));
  copy_parent_node->Insert(key, new_node->GetPageId(), comparator_);
  auto parent_new_split_node = InternalSplit(copy_parent_node);
  new_node->SetParentPageId(parent_new_split_node->GetPageId());  // 既然分裂 非根节点就一定有key上移
  KeyType new_risen_key = parent_new_split_node->KeyAt(0);  // 因为key0无效，这里包含非叶子的key的上移过程
  std::memcpy(parent_page->GetData(), mem,
              INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType) * (copy_parent_node->GetSize()));
  parent_page_node->SetSize(copy_parent_node->GetSize());
  InsertIntoParent(parent_page_node, new_risen_key, parent_new_split_node);
  delete[] mem;
  buffer_pool_manager_->UnpinPage(parent_page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafSplit(LeafPage *leaf_node) -> LeafPage * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_leaf_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf_node->Init(new_page_id, -1, leaf_max_size_);
  int old_size = leaf_node->GetSize();
  int half = old_size / 2;
  for (int i = half; i < old_size; i++) {
    new_leaf_node->InsertAfter(leaf_node->KeyAt(i), leaf_node->ValueAt(i));
    // 删除原来的？TODO:
  }
  leaf_node->SetSize(half);  // 超过size的内存是否需要手动释放？ TODO:
  new_leaf_node->SetSize(old_size - half);

  new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(new_page_id);

  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalSplit(InternalPage *internal_page) -> InternalPage * {
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_internal_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_internal_node->Init(new_page_id, -1, internal_max_size_);
  // 既然分裂，那么key0是要上升的，既然key0无效 直接放在Index0就好了
  int old_size = internal_page->GetSize();
  int half = 1 + (old_size - 1) / 2;
  for (int i = half; i < internal_page->GetSize(); i++) {
    new_internal_node->InsertAfter(internal_page->KeyAt(i), internal_page->ValueAt(i));
  }
  internal_page->SetSize(half);
  new_internal_node->SetSize(old_size - half);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_internal_node;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }
  LeafPage *leaf_node = FindLeaf(key);
  int leaf_old_size = leaf_node->GetSize();
  assert(leaf_old_size > 0);
  KeyType old_key0 = leaf_node->KeyAt(0);
  leaf_node->Remove(key, comparator_);
  if (leaf_node->IsRootPage()) {
    return;
  }
  // 1 oldsize > minsize ,直接删，更新父节点Key值
  if (leaf_old_size > leaf_node->GetMinSize()) {
    assert(leaf_old_size > 1);
    // 由于内部节点不存在相等的key，只有叶子节点最多需要更新父节点Key一次 不用递归？
    if (comparator_(old_key0, key) == 0) {
      UpdataParentKey(key, leaf_node->KeyAt(0), leaf_node);
    }
    return;
  }
  // 2 oldsize<=minsize
  // 左右兄弟叶子节点有>minsize，分一分，更新父节点key（因为要递归执行 放在MergeBrother函数中了）
  //  if(RedistributeBrother(key,leaf_node)){
  //    return;
  //  }
  // 左右兄弟节点都<=minsize,合并，删除父节点中对应的key，更新父父节点key，递归此过程（根节点另外判断size>1)。
  MergeBrother(key, leaf_node);
}

/* ==============My function for remove================*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdataParentKey(KeyType old_key, KeyType new_key, BPlusTreePage *cur_node) {
  page_id_t parent_id = cur_node->GetParentPageId();
  // if -1 ，根节点,上层已经判断过 不可能存在
  assert(parent_id);
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page);
  int index = parent_node->FindIndex(old_key, comparator_) - 1;
  assert(cur_node->GetPageId() == parent_node->ValueAt(index));
  bool dirty = false;
  if (comparator_(parent_node->KeyAt(index), old_key) == 0) {
    parent_node->SetKeyAt(index, new_key);
    dirty = true;
  }
  buffer_pool_manager_->UnpinPage(parent_id, dirty);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RedistributeBrother(const KeyType &key, BPlusTreePage *cur_node) -> bool {
  page_id_t parent_id = cur_node->GetParentPageId();
  // if -1 ，根节点,上层已经判断过 不可能存在
  assert(parent_id);
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page);
  // 定位 到父节点中 指向cur_node的key：
  int index = parent_node->FindIndex(key, comparator_) - 1;
  assert(cur_node->GetPageId() == parent_node->ValueAt(index));
  // 优先与左子节点redistribute？？ //TODO:
  if (index - 1 > 0) {
    // 左兄弟节点存在
    page_id_t left_brother_page_id = parent_node->ValueAt(index - 1);
    Page *left_brother_page = buffer_pool_manager_->FetchPage(left_brother_page_id);
    auto left_brother_node = reinterpret_cast<BPlusTreePage *>(left_brother_page);
    // 左兄弟是富哥
    if (left_brother_node->GetSize() > left_brother_node->GetMinSize()) {
      // 判断节点类型,处理方式不同
      // 叶子节点： 左边借过来之后直接UpdateParentKey就行
      if (left_brother_node->IsLeafPage()) {
        auto left_brother_leaf_node = reinterpret_cast<LeafPage *>(left_brother_node);
        auto cur_leaf_node = reinterpret_cast<LeafPage *>(cur_node);
        cur_leaf_node->PushFront(left_brother_leaf_node->PopBack());
        // UpdateParentKey
        parent_node->SetKeyAt(index, cur_leaf_node->KeyAt(0));
      } else {
        // 内部节点： 需要左边上升到父节点 父节点的Key借过来,重点注意value的变化
        auto left_brother_inner_node = reinterpret_cast<InternalPage *>(left_brother_node);
        auto cur_inner_node = reinterpret_cast<InternalPage *>(cur_node);
        auto kv = left_brother_inner_node->PopBack();
        // Update ParentKey 下移
        KeyType parent_key = parent_node->KeyAt(index);
        parent_node->SetKeyAt(index, kv.first);
        cur_inner_node->SetKeyAt(0, parent_key);
        cur_inner_node->PushFront(kv);
      }
      buffer_pool_manager_->UnpinPage(left_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    }
  }
  if (index + 1 < parent_node->GetSize()) {
    // 右兄弟节点存在
    page_id_t right_brother_page_id = parent_node->ValueAt(index + 1);
    Page *right_brother_page = buffer_pool_manager_->FetchPage(right_brother_page_id);
    auto right_brother_node = reinterpret_cast<BPlusTreePage *>(right_brother_page);
    // 右兄弟是富哥
    if (right_brother_node->GetSize() > right_brother_node->GetMinSize()) {
      // 叶子节点： 右边借过来之后直接Update 下一个Index的ParentKey就行
      if (right_brother_node->IsLeafPage()) {
        auto right_brother_leaf_node = reinterpret_cast<LeafPage *>(right_brother_node);
        auto cur_leaf_node = reinterpret_cast<LeafPage *>(cur_node);
        auto kv = right_brother_leaf_node->PopFront();
        cur_leaf_node->PushBack(kv);
        parent_node->SetKeyAt(index + 1, right_brother_leaf_node->KeyAt(0));
      } else {
        // 内部节点： 需要右边上升到父节点 父节点的Key借过来,重点注意value的变化
        auto right_brother_inner_node = reinterpret_cast<InternalPage *>(right_brother_node);
        auto cur_inner_node = reinterpret_cast<InternalPage *>(cur_node);
        KeyType parent_key = parent_node->KeyAt(index + 1);
        right_brother_inner_node->SetKeyAt(0, parent_key);
        auto kv = right_brother_inner_node->PopFront();
        parent_node->SetKeyAt(index + 1, right_brother_inner_node->KeyAt(0));
        cur_inner_node->PushBack(kv);
      }
      buffer_pool_manager_->UnpinPage(right_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_id, true);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(parent_id, false);
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeBrother(const KeyType &key, BPlusTreePage *cur_node) {
  // 递归终止条件
  if (cur_node->IsRootPage()) {
    assert(!cur_node->IsLeafPage());
    // 如果没有key了，被子节点顶替
    if (cur_node->GetSize() == 1) {
      auto old_root = reinterpret_cast<InternalPage *>(cur_node);
      page_id_t new_root_page_id = old_root->ValueAt(0);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = new_root_page_id;
      Page *new_root = buffer_pool_manager_->FetchPage(new_root_page_id);
      auto new_root_node = reinterpret_cast<BPlusTreePage *>(new_root->GetData());
      new_root_node->SetParentPageId(-1);
      UpdateRootPageId(0);
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    }
    return;
  }

  if (cur_node->GetSize() >= cur_node->GetMinSize()) {
    return;
  }
  if (RedistributeBrother(key, cur_node)) {
    // 如果能重分配均匀，则不需要merge节点
    return;
  }
  page_id_t parent_page_id = cur_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page);
  // 定位 到父节点中 指向cur_node的key：
  int index = parent_node->FindIndex(key, comparator_) - 1;
  assert(cur_node->GetPageId() == parent_node->ValueAt(index));

  // 优先与左兄弟节点合并
  if (index - 1 > 0) {
    page_id_t left_brother_page_id = parent_node->ValueAt(index - 1);
    Page *left_brother_page = buffer_pool_manager_->FetchPage(left_brother_page_id);
    auto left_brother_node = reinterpret_cast<BPlusTreePage *>(left_brother_page);

    // 错误的陷阱：不用判断size，因为merge是在调用完Redistribute失败后调用，存在就一定能合并
    // assert(left_brother_node->GetSize()>=left_brother_node->GetMinSize());
    // assert(cur_node->GetSize()<cur_node->GetMinSize());

    // 需要判断size，因为会存在向上的递归调用。
    // 非叶子节点可以满，叶子节点不可以满，所以在判断完类型后再判断size

    // 叶子节点的合并
    if (cur_node->IsLeafPage() && (left_brother_node->GetSize() + cur_node->GetSize() < cur_node->GetMaxSize())) {
      auto left_leaf_node = reinterpret_cast<LeafPage *>(left_brother_node);
      auto cur_leaf_node = reinterpret_cast<LeafPage *>(cur_node);
      int cur_size = cur_leaf_node->GetSize();
      for (int i = 0; i < cur_size; i++) {
        left_leaf_node->PushBack(std::make_pair(cur_leaf_node->KeyAt(i), cur_leaf_node->ValueAt(i)));
      }
      cur_leaf_node->SetSize(0);
      parent_node->RemoveByIndex(index);
      MergeBrother(key, parent_node);
      buffer_pool_manager_->UnpinPage(left_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      assert(buffer_pool_manager_->DeletePage(cur_leaf_node->GetPageId()));
      return;
    }
    // 内部节点的合并
    if (!cur_node->IsLeafPage() && (left_brother_node->GetSize() + cur_node->GetSize() <= cur_node->GetMaxSize())) {
      auto left_inner_node = reinterpret_cast<InternalPage *>(left_brother_node);
      auto cur_inner_node = reinterpret_cast<InternalPage *>(cur_node);
      int cur_size = cur_inner_node->GetSize();
      cur_inner_node->SetKeyAt(0,parent_node->KeyAt(index));
      for (int i = 0; i < cur_size; i++) {
        left_inner_node->PushBack(std::make_pair(left_inner_node->KeyAt(i), left_inner_node->ValueAt(i)));
      }
      cur_inner_node->SetSize(0);
      parent_node->RemoveByIndex(index);
      MergeBrother(key, parent_node);
      buffer_pool_manager_->UnpinPage(left_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      assert(buffer_pool_manager_->DeletePage(cur_inner_node->GetPageId()));
      return;
    }
    buffer_pool_manager_->UnpinPage(parent_page_id, false);
  }
  // 与右兄弟节点合并
  if (index + 1 < parent_node->GetSize()) {
    page_id_t right_brother_page_id = parent_node->ValueAt(index + 1);
    Page *right_brother_page = buffer_pool_manager_->FetchPage(right_brother_page_id);
    auto right_brother_node = reinterpret_cast<BPlusTreePage *>(right_brother_page);

    // 叶子节点的合并
    if (cur_node->IsLeafPage() && (right_brother_node->GetSize() + cur_node->GetSize() < cur_node->GetMaxSize())) {
      auto right_leaf_node = reinterpret_cast<LeafPage *>(right_brother_node);
      auto cur_leaf_node = reinterpret_cast<LeafPage *>(cur_node);
      int right_leaf_node_size = right_leaf_node->GetSize();
      for (int i = 0; i < right_leaf_node_size; i++) {
        cur_leaf_node->PushBack(std::make_pair(right_leaf_node->KeyAt(i), right_leaf_node->ValueAt(i)));
      }
      right_leaf_node->SetSize(0);
      parent_node->RemoveByIndex(index + 1);
      MergeBrother(key, parent_node);
      buffer_pool_manager_->UnpinPage(right_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      assert(buffer_pool_manager_->DeletePage(right_leaf_node->GetPageId()));
      return;
    }
    // 内部节点的合并
    if (!cur_node->IsLeafPage() && (right_brother_node->GetSize() + cur_node->GetSize() <= cur_node->GetMaxSize())) {
      auto right_inner_node = reinterpret_cast<InternalPage *>(right_brother_node);
      auto cur_inner_node = reinterpret_cast<InternalPage *>(cur_node);
      int right_inner_node_size = right_inner_node->GetSize();
      right_inner_node->SetKeyAt(0,parent_node->KeyAt(index+1));
      for (int i = 0; i < right_inner_node_size; i++) {
        cur_inner_node->PushBack(std::make_pair(right_inner_node->KeyAt(i), right_inner_node->ValueAt(i)));
      }
      right_inner_node->SetSize(0);
      parent_node->RemoveByIndex(index + 1);
      MergeBrother(key, parent_node);
      buffer_pool_manager_->UnpinPage(right_brother_page_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      assert(buffer_pool_manager_->DeletePage(right_brother_node->GetPageId()));
      return;
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id, false);
  assert(0);  // 如果这里报错 说明没有正确merge 否则前面就return了 不可能所有条件都不满足到这里
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  assert(!IsEmpty());
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto cur_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  while (!cur_node->IsLeafPage()) {
    auto inner_node = reinterpret_cast<InternalPage *>(cur_node);
    auto page_id = inner_node->ValueAt(0);
    Page *next_page = buffer_pool_manager_->FetchPage(page_id);
    cur_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(cur_node), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  assert(!IsEmpty());
  LeafPage *leaf_node = FindLeaf(key);
  int index = leaf_node->FindIndex(key, comparator_);
  return INDEXITERATOR_TYPE(leaf_node, index, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  assert(!IsEmpty());
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto cur_node = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  while (!cur_node->IsLeafPage()) {
    auto inner_node = reinterpret_cast<InternalPage *>(cur_node);
    auto page_id = inner_node->ValueAt(inner_node->GetSize() - 1);
    Page *next_page = buffer_pool_manager_->FetchPage(page_id);
    cur_node = reinterpret_cast<BPlusTreePage *>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  int max_index = cur_node->GetSize() - 1;
  return INDEXITERATOR_TYPE(reinterpret_cast<LeafPage *>(cur_node), max_index + 1, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
