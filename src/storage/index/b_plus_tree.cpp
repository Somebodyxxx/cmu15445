#include <cassert>
#include <cstring>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_==-1;
}
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
  if(IsEmpty()){
    return false;
  }
  LeafPage* leafnode = FindLeaf(key);
  ValueType v;
  if(!leafnode->FindKey(key,v,comparator_)){
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
  if(IsEmpty()){
    Page* new_page = buffer_pool_manager_->NewPage(&root_page_id_);
    assert(new_page);
    auto leaf_page = reinterpret_cast<LeafPage*>(new_page->GetData());
    leaf_page->Init(root_page_id_,-1,leaf_max_size_);
    leaf_page->Insert(key,value,comparator_);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    UpdateRootPageId(1);
    return true;
  }
  //判断重复
  std::vector<ValueType> result;
  if(GetValue(key, &result)){
    return false;
  }
  InsertIntoLeaf(key,value);
  return true;
}

/* =========================my function =======================*/
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key) -> LeafPage*{
  Page* root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto cur_node = reinterpret_cast<BPlusTreePage*>(root_page->GetData());
  while(!cur_node->IsLeafPage()){
    auto inner_node = reinterpret_cast<InternalPage*>(cur_node);
    int index = inner_node->FindIndex(key,comparator_);//返回第一个比key大的位置
    auto page_id = inner_node->ValueAt(index-1);
    Page* next_page = buffer_pool_manager_->FetchPage(page_id);
    cur_node = reinterpret_cast<BPlusTreePage*>(next_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id,false);
  }
  //cur_node is LeafPage
  buffer_pool_manager_->UnpinPage(root_page_id_,false);
  return reinterpret_cast<LeafPage*>(cur_node);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value){
  LeafPage* leaf_node = FindLeaf(key);
  leaf_node->Insert(key,value,comparator_);
  if(leaf_node->GetSize() < leaf_max_size_){
    //释放资源操作
    return;
  }
  //分裂+重组
  auto new_leaf_node = LeafSplit(leaf_node);//
  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0),new_leaf_node);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage* old_node,const KeyType& key,BPlusTreePage* new_node){
  if(old_node->IsRootPage()){
    auto new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
    auto new_root = reinterpret_cast<InternalPage*>(new_root_page->GetData());
    new_root->Init(root_page_id_,-1,internal_max_size_);
    new_root->SetValue0(old_node->GetPageId());
    new_root->InsertAfter(key,new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    return;
  }
  page_id_t parent_page_id = old_node->GetParentPageId();
  auto parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
  assert(parent_page);
  auto parent_page_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
  if(parent_page_node->GetSize()<internal_max_size_){
    parent_page_node->Insert(key,new_node->GetPageId(),comparator_);
    new_node->SetParentPageId(parent_page_id);
    buffer_pool_manager_->UnpinPage(parent_page_id,true);
    return;
  }
 
  auto mem = new char[INTERNAL_PAGE_HEADER_SIZE + sizeof(MappingType)*(parent_page_node->GetSize()+1)];
  auto copy_parent_node = reinterpret_cast<InternalPage*>(mem);
  std::memcpy(mem, parent_page->GetData(),INTERNAL_PAGE_HEADER_SIZE+sizeof(MappingType)*(parent_page_node->GetSize()));
  copy_parent_node->Insert(key,new_node->GetPageId(),comparator_);
  auto parent_new_split_node = InternalSplit(copy_parent_node);
  new_node->SetParentPageId(parent_new_split_node->GetPageId());//既然分裂 非根节点就一定有key上移
  KeyType new_risen_key = parent_new_split_node->KeyAt(0);//因为key0无效，这里包含非叶子的key的上移过程
  std::memcpy(parent_page->GetData(),mem,INTERNAL_PAGE_HEADER_SIZE+sizeof(MappingType)*(copy_parent_node->GetSize()));
  parent_page_node->SetSize(copy_parent_node->GetSize());
  InsertIntoParent(parent_page_node, new_risen_key, parent_new_split_node);
  delete [] mem;
  buffer_pool_manager_->UnpinPage(parent_page_id,true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::LeafSplit(LeafPage* leaf_node) -> LeafPage*{
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_leaf_node = reinterpret_cast<LeafPage*>(new_page->GetData());
  new_leaf_node->Init(new_page_id,-1,leaf_max_size_);
  int old_size = leaf_node->GetSize();
  int half = old_size/2;
  for(int i = half;i<old_size;i++){
    new_leaf_node->InsertAfter(leaf_node->KeyAt(i),leaf_node->ValueAt(i));
    //删除原来的？TODO:
  }
  leaf_node->SetSize(half);//超过size的内存是否需要手动释放？ TODO:
  new_leaf_node->SetSize(old_size-half);

  new_leaf_node->SetNextPageId(leaf_node->GetNextPageId());
  leaf_node->SetNextPageId(new_page_id);

  buffer_pool_manager_->UnpinPage(new_page_id,true);
  return new_leaf_node;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InternalSplit(InternalPage* internal_page) -> InternalPage*{
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_internal_node = reinterpret_cast<InternalPage*>(new_page->GetData());
  new_internal_node->Init(new_page_id,-1,internal_max_size_);
  //既然分裂，那么key0是要上升的，既然key0无效 直接放在Index0就好了
  int old_size = internal_page->GetSize();
  int half = 1+(old_size-1)/2;
  for(int i = half;i<internal_page->GetSize();i++){
    new_internal_node->InsertAfter(internal_page->KeyAt(i),internal_page->ValueAt(i));
  }
  internal_page->SetSize(half);
  new_internal_node->SetSize(old_size-half);
  buffer_pool_manager_->UnpinPage(new_page_id,true);
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

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
