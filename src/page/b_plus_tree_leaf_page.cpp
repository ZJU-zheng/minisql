#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/


/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageType(IndexPageType::LEAF_PAGE);
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}


/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
    int left = 0;
    int right = GetSize()-1;
    int mid,comp_result;
    while(left <= right){
        mid = (left+right)/2;
        comp_result = KM.CompareKeys(key,KeyAt(mid));
        if(comp_result == 0){//since unique,just return is ok
            return mid;
        }
        else if(comp_result < 0){
            right = mid;
        }
        else{
            left = mid + 1;
        }
        if(right == left){
            return left;
        }
    }
    LOG(WARNING)<<"unknown mistake in KeyIndex in leafpage"<<std::endl;
    return INVALID_PAGE_ID;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
    GenericKey *key = KeyAt(index);
    RowId rowid = ValueAt(index);
    return make_pair(key, rowid);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
    int i;
    int size = GetSize();
    int old_value_index = KeyIndex(key,KM);
    GenericKey *pre_key;
    RowId pre_rowid;
    for(i = (size-1);i >= old_value_index;i--){
        pre_key = KeyAt(i);
        pre_rowid = ValueAt(i);
        SetKeyAt(i+1, pre_key);
        SetValueAt(i+1, pre_rowid);
    }
    SetKeyAt(old_value_index, key);
    SetValueAt(old_value_index, value);
    SetSize(size + 1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
    int size = GetSize();
    recipient->CopyNFrom(PairPtrAt(size/2+1),size/2);
    SetSize(size-size/2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
    memcpy(data_,src,size*(GetKeySize() + sizeof(RowId)));
    SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
    int index = KeyIndex(key,KM);
    int comp_result = KM.CompareKeys(key,KeyAt(index));
    if(comp_result == 0){
        value = ValueAt(index);
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
    int size = GetSize(),i;
    int index = KeyIndex(key,KM);
    int comp_result = KM.CompareKeys(key,KeyAt(index));
    GenericKey *temp1;
    RowId temp2;
    if(comp_result == 0){
        for(i = index;i < (size-1);i++){
            temp1 = KeyAt(i+1);
            temp2 = ValueAt(i+1);
            SetKeyAt(i,temp1);
            SetValueAt(i,temp2);
        }
        SetSize(size-1);
        return GetSize();
    }
    else{
        LOG(WARNING)<<"delete failed"<<std::endl;
        return size;
    }
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
//when merging,I ask the back to move all to the front
void LeafPage::MoveAllTo(LeafPage *recipient) {
    int size = GetSize();
    int s_size = recipient->GetSize();
    recipient->PairCopy(recipient->KeyAt(s_size), KeyAt(0),size);
    recipient->SetNextPageId(GetNextPageId());
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
}