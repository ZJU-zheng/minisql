#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"


BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  if(leaf_max_size_ == 0)
    leaf_max_size_ = LEAF_PAGE_SIZE;
  if(internal_max_size_ == 0)
    internal_max_size_ = INTERNAL_PAGE_SIZE;
  auto page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_id;
  if(page->GetRootId(index_id,&root_id)){
    root_page_id_ = root_id;
  }
  else{
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
}
/**
 * TODO: Student Implement
 */
void BPlusTree::Destroy(page_id_t current_page_id) {
  if(root_page_id_ != INVALID_PAGE_ID){

  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID)
    return true;
  else
    return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(root_page_id_ == INVALID_PAGE_ID){
    return false;
  }
  BPlusTreeLeafPage *temp = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key,false));
  if(temp->GetSize() == 0){
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    return false;
  }
  int index = temp->KeyIndex(key,processor_);
  if(index >= temp->GetSize()){
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    return false;
  }
  if(index == INVALID_PAGE_ID){
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    return false;
  }
  if(processor_.CompareKeys(temp->KeyAt(index),key)==0){
    result.push_back(temp->ValueAt(index));
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
  return false;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(root_page_id_ == INVALID_PAGE_ID){
    StartNewTree(key,value);
    return true;
  }
  else{
    return InsertIntoLeaf(key,value,transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  page_id_t page_id;
  BPlusTreeLeafPage *page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(page_id));
  if(page == nullptr){
    LOG(ERROR)<<"get page failed"<<std::endl;
    return;
  }
  root_page_id_ = page_id;
  UpdateRootPageId(1);
  page->Init(page_id,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
  page->SetNextPageId(INVALID_PAGE_ID);
  page->Insert(key,value,processor_);
  buffer_pool_manager_->UnpinPage(page_id,true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  BPlusTreeLeafPage * page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key,false));
  BPlusTreeLeafPage *new_split_page;
  int size = page->Insert(key,value,processor_);
  if(size == -1){
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    LOG(WARNING)<<"insert failed"<<std::endl;
    return false;
  }
  if(size > leaf_max_size_){
    new_split_page = Split(page,transaction);
    InsertIntoParent(page,new_split_page->KeyAt(0),new_split_page,transaction);
    return true;
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
//split后new_page的key[0]还有值，可以往上传
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t page_id;
  BPlusTreeInternalPage * new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(page_id));
  if(new_page == nullptr){
    LOG(ERROR)<<"get page failed"<<std::endl;
    return nullptr;
  }
  new_page->Init(page_id,node->GetParentPageId(),processor_.GetKeySize(),leaf_max_size_);
  node->MoveHalfTo(new_page,buffer_pool_manager_);
  return new_page;
}
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t page_id,next_page_id;
  BPlusTreeLeafPage * new_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(page_id));
  if(new_page == nullptr){
    LOG(ERROR)<<"get page failed"<<std::endl;
    return nullptr;
  }
  new_page->Init(page_id,node->GetParentPageId(),processor_.GetKeySize(),leaf_max_size_);
  next_page_id = node->GetNextPageId();
  node->MoveHalfTo(new_page);
  node->SetNextPageId(new_page->GetPageId());
  new_page->SetNextPageId(next_page_id);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if(old_node->IsRootPage()){
    page_id_t page_id;
    auto new_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(page_id));
    new_page->Init(page_id,INVALID_PAGE_ID,processor_.GetKeySize(),leaf_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(page_id,true);
    root_page_id_ = page_id;
    UpdateRootPageId(0);
    old_node->SetParentPageId(page_id);
    new_node->SetParentPageId(page_id);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
  }
  else{
    page_id_t page_id = old_node->GetParentPageId();
    BPlusTreeInternalPage *new_page;
    auto parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(page_id));
    new_node->SetParentPageId(page_id);
    int size = parent_page->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    if(size > internal_max_size_){
      new_page = reinterpret_cast<BPlusTreeInternalPage *>(Split(parent_page,transaction));
      InsertIntoParent(parent_page,new_page->KeyAt(0),new_page,transaction);
      return;
    }
    buffer_pool_manager_->UnpinPage(page_id,true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if(root_page_id_ == INVALID_PAGE_ID){
    return;
  }
  LeafPage* page = reinterpret_cast<LeafPage *>(FindLeafPage(key,false));
  if(page == nullptr){
    LOG(WARNING)<<"page is nullptr"<<std::endl;
    return;
  }
  int size1 = page->GetSize();
  int size2 = page->RemoveAndDeleteRecord(key,processor_);
  if(size2 == -1){
    LOG(WARNING)<<"only for test:this page has nothing"<<std::endl;
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return;
  }
  if(size1 == size2){
    LOG(WARNING)<<"no such key to delete"<<std::endl;
    buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return;
  }
  if(page->IsRootPage()){
    int size = page->GetSize();
    if((AdjustRoot(page))&&(size == 0)){
      LOG(WARNING)<<"there is nothing in the b+ tree now"<<std::endl;
      buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
      if(!buffer_pool_manager_->DeletePage(page->GetPageId())){
        LOG(WARNING)<<"it is you"<<std::endl;
      }
      return;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
    return;
  }
  if(size2 < page->GetMinSize()){
    if(!CoalesceOrRedistribute(page,transaction)){
      LOG(WARNING)<<"big mistake"<<std::endl;
    }
    return;
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page->GetParentPageId()));
  int index = parent->ValueIndex(page->GetPageId());
  parent->SetKeyAt(index,page->KeyAt(0));
  buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
bool BPlusTree::CoalesceOrRedistribute(LeafPage *&node, Transaction *transaction) {
  if(node->IsRootPage()){
    if(AdjustRoot(node)){
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      if(!buffer_pool_manager_->DeletePage(node->GetPageId())){
        LOG(WARNING)<<"it it you"<<std::endl;
      }
      return true;
    }
    LOG(WARNING)<<"Unknown mistake"<<std::endl;
    return false;
  }
  InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  int size = parent->GetSize();
  int index = parent->ValueIndex(node->GetPageId());
  if(node->IsLeafPage()){
    parent->SetKeyAt(index,node->KeyAt(0));
  }
  if(index == 0){
    LeafPage *sibling = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1)));
    if((sibling->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      Coalesce(sibling,node,parent,index,transaction);
    }
    else{
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling,node,0);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(),true);
    }
  }
  else if(index == (size-1)){
    LeafPage *sibling = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(size-2)));
    if((sibling->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      Coalesce(node,sibling,parent,index-1,transaction);
    }
    else{
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling,node,1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(),true);
    }
  }
  else{
    LeafPage *sibling1 = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index-1)));
    LeafPage *sibling2 = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index+1)));
    if((sibling1->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      buffer_pool_manager_->UnpinPage(sibling2->GetPageId(),false);
      Coalesce(node,sibling1,parent,index-1,transaction);
    }
    else if((sibling2->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      buffer_pool_manager_->UnpinPage(sibling1->GetPageId(),true);
      Coalesce(sibling2,node,parent,index,transaction);
    }
    else {
      buffer_pool_manager_->UnpinPage(sibling2->GetPageId(),false);
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling1,node,1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling1->GetPageId(),true);
    }
  }
  return true;
}

bool BPlusTree::CoalesceOrRedistribute(InternalPage *&node, Transaction *transaction) {
  if(node->IsRootPage()){
    if(AdjustRoot(node)){
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      if(!buffer_pool_manager_->DeletePage(node->GetPageId())){
        LOG(WARNING)<<"it it you"<<std::endl;
      }
      return true;
    }
    LOG(WARNING)<<"Unknown mistake"<<std::endl;
    return false;
  }
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
  int size = parent->GetSize();
  int index = parent->ValueIndex(node->GetPageId());
  if(node->IsLeafPage()){
    parent->SetKeyAt(index,node->KeyAt(0));
  }
  //node and parent not unpin
  if(index == 0){
    InternalPage *sibling = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(1)));
    if((sibling->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      Coalesce(sibling,node,parent,index,transaction);
    }
    else{
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling,node,0);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(),true);
    }
  }
  else if(index == (size-1)){
    InternalPage *sibling = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(size-2)));
    if((sibling->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      Coalesce(node,sibling,parent,index-1,transaction);
    }
    else{
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling,node,1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(),true);
    }
  }
  else{
    InternalPage *sibling1 = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index-1)));
    InternalPage *sibling2 = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index+1)));
    if((sibling1->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      buffer_pool_manager_->UnpinPage(sibling2->GetPageId(),false);
      Coalesce(node,sibling1,parent,index-1,transaction);
    }
    else if((sibling2->GetSize()+node->GetSize()) <= node->GetMaxSize()){
      buffer_pool_manager_->UnpinPage(sibling1->GetPageId(),true);
      Coalesce(sibling2,node,parent,index,transaction);
    }
    else {
      buffer_pool_manager_->UnpinPage(sibling2->GetPageId(),false);
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(),true);
      Redistribute(sibling1,node,1);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
      buffer_pool_manager_->UnpinPage(sibling1->GetPageId(),true);
    }
  }
  return true;
}
/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction){
  GenericKey *middle_key = parent->KeyAt(index+1);
  neighbor_node->MoveAllTo(node,middle_key,buffer_pool_manager_);
  parent->Remove(index+1);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
  if(!buffer_pool_manager_->DeletePage(neighbor_node->GetPageId())){
    //LOG(WARNING)<<"it is you"<<std::endl;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
  if(parent->GetSize() < parent->GetMinSize()){
    CoalesceOrRedistribute(parent,transaction);
  }
  else{
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  }
  return true;
}

bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction){
  neighbor_node->MoveAllTo(node);
  parent->Remove(index+1);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
  if(!buffer_pool_manager_->DeletePage(neighbor_node->GetPageId())){
    LOG(WARNING)<<"it is you"<<std::endl;
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
  if(parent->GetSize() < parent->GetMinSize()){
    CoalesceOrRedistribute(parent,transaction);
  }
  else{
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  }
  return true;
}

/*
template <typename N>
bool BPlusTree::Coalesce(N *&neighbor_node, N *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
    if(node->IsLeafPage()){
        BPlusTreeLeafPage *temp1 = reinterpret_cast<BPlusTreeLeafPage *>(neighbor_node);
        BPlusTreeLeafPage *temp2 = reinterpret_cast<BPlusTreeLeafPage *>(node);
        temp1->MoveAllTo(temp2);
        parent->Remove(index+1);
    }
    else{
        GenericKey *middle_key = parent->KeyAt(index+1);
        BPlusTreeInternalPage *temp1 = reinterpret_cast<BPlusTreeInternalPage *>(neighbor_node);
        BPlusTreeInternalPage *temp2 = reinterpret_cast<BPlusTreeInternalPage *>(node);
        temp1->MoveAllTo(temp2,middle_key,buffer_pool_manager_);
        parent->Remove(index+1);
    }
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(),true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    if(parent->GetSize() < parent->GetMinSize()){
        CoalesceOrRedistribute(parent,transaction);
    }
    return true;
}
*/
/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  if(index == 0){
    InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    neighbor_node->MoveFirstToEndOf(node);
    int index_ = parent->ValueIndex(neighbor_node->GetPageId());
    parent->SetKeyAt(index_,neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  }
  else{
    InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    neighbor_node->MoveLastToFrontOf(node);
    int index_ = parent->ValueIndex(node->GetPageId());
    parent->SetKeyAt(index_,node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  GenericKey *middle_key;
  if(index == 0){
    InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index_ = parent->ValueIndex(neighbor_node->GetPageId());
    middle_key = parent->KeyAt(index_);
    neighbor_node->MoveFirstToEndOf(node,middle_key,buffer_pool_manager_);
    parent->SetKeyAt(index_,neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
    //Row temp(INVALID_ROWID);
    //processor_.DeserializeToKey(middle_key,temp,processor_.GetSchema());
    //LOG(WARNING)<<temp.GetField(0)->toString()<<std::endl;
  }
  else{
    InternalPage *parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index_ = parent->ValueIndex(node->GetPageId());
    middle_key = parent->KeyAt(index_);
    neighbor_node->MoveLastToFrontOf(node,middle_key,buffer_pool_manager_);
    parent->SetKeyAt(index_,node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent->GetPageId(),true);
    //Row temp(INVALID_ROWID);
    //processor_.DeserializeToKey(middle_key,temp,processor_.GetSchema());
    //LOG(WARNING)<<temp.GetField(0)->toString()<<std::endl;
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(old_root_node->GetSize() == 0){
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  if(old_root_node->GetSize() == 1){
    BPlusTreeInternalPage *temp = reinterpret_cast<BPlusTreeInternalPage *>(old_root_node);
    root_page_id_ = temp->ValueAt(0);
    UpdateRootPageId(0);
    temp = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    temp->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_,true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  BPlusTreeLeafPage *temp = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr,true));
  page_id_t pageId = temp->GetPageId();
  buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
  return IndexIterator(pageId,buffer_pool_manager_,0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  BPlusTreeLeafPage *temp = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key,false));
  page_id_t pageId = temp->GetPageId();
  int index = temp->KeyIndex(key,processor_);
  buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
  return IndexIterator(pageId,buffer_pool_manager_,index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  BPlusTreeLeafPage *temp = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr,true));
  BPlusTreeLeafPage *next;
  while(temp->GetNextPageId() != INVALID_PAGE_ID){
    next=reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(temp->GetNextPageId()));
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    temp=next;
  }
  page_id_t pageId = temp->GetPageId();
  int index=temp->GetSize();
  buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
  return IndexIterator(pageId,buffer_pool_manager_,index-1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, bool leftMost) {
  if(root_page_id_ == INVALID_PAGE_ID){
    return nullptr;
  }
  BPlusTreePage * page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  BPlusTreeInternalPage *temp;
  Page *result;
  page_id_t temp_page;
  if(leftMost){
    while(!page->IsLeafPage()){
      temp = reinterpret_cast<BPlusTreeInternalPage *>(page);
      temp_page = temp->ValueAt(0);
      page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(temp_page));
      buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
    }
    result = reinterpret_cast<Page *>(page);
    return result;
  }
  while(!page->IsLeafPage()){
    temp = reinterpret_cast<BPlusTreeInternalPage *>(page);
    temp_page = temp->Lookup(key,processor_);
    page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(temp_page));
    buffer_pool_manager_->UnpinPage(temp->GetPageId(),false);
  }
  result = reinterpret_cast<Page *>(page);
  return result;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) const {
  auto page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record){
    page->Insert(index_id_,root_page_id_);
  }
  else{
    page->Update(index_id_,root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row temp(INVALID_ROWID);
      processor_.DeserializeToKey(leaf->KeyAt(i),temp,processor_.GetSchema());
      out << "<TD>" << temp.GetField(0)->toString() << "</TD>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row temp(INVALID_ROWID);
        processor_.DeserializeToKey(inner->KeyAt(i),temp,processor_.GetSchema());
        out <<  temp.GetField(0)->toString();
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
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}