#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()


/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    int left = 1;
    int right = GetSize()-1;
    int mid,comp_result;
    while(left <= right){
        mid = (left+right)/2;
        comp_result = KM.CompareKeys(key,KeyAt(mid));
        if(comp_result == 0){//equals
            left = mid + 1;
        }
        else if(comp_result < 0){
            right = mid - 1;
        }
        else{
            left = mid + 1;
        }
    }
    return ValueAt(left-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    SetSize(2);
    SetValueAt(0,old_value);
    SetKeyAt(1,new_key);
    SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    int i;
    int size = GetSize();
    int old_value_index = ValueIndex(old_value);
    GenericKey *pre_key;
    page_id_t pre_value;
    for(i = (size-1);i > old_value_index;i--){
        pre_key = KeyAt(i);
        pre_value = ValueAt(i);
        SetKeyAt(i+1, pre_key);
        SetValueAt(i+1, pre_value);
    }
    SetKeyAt(old_value_index + 1, new_key);
    SetValueAt(old_value_index + 1, new_value);
    SetSize(size + 1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    int offset = size-size/2;
    recipient->CopyNFrom(KeyAt((size+1)/2) ,size/2,buffer_pool_manager);
    SetSize(offset);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    SetSize(size);
    memcpy(data_,src,size*(GetKeySize()+sizeof(page_id_t)));
    int i;
    InternalPage *temp;
    for(i = 0;i < size;i++){
        temp = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(ValueAt(i)));
        temp->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(ValueAt(i),true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int i;
    int size = GetSize();
    GenericKey* temp1;
    page_id_t temp2;
    for(i = index;i < (GetSize()-1);i++){
        temp1 = KeyAt(i+1);
        temp2 = ValueAt(i+1);
        SetKeyAt(i,temp1);
        SetValueAt(i,temp2);
    }
    SetSize(size-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    SetSize(0);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
//when merging,I ask the back to move all to the front
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0,middle_key);
    int size = GetSize();
    int size_ = recipient->GetSize();
    memcpy(recipient->KeyAt(size_),data_,size*(GetKeySize()+sizeof(page_id_t)));
    recipient->SetSize(size_ + size);
    int i;
    InternalPage *temp;
    for(i = 0;i < size;i++){
        temp = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(ValueAt(i)));
        temp->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(ValueAt(i),true);
    }
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(0,middle_key);
    recipient->CopyLastFrom(KeyAt(0), ValueAt(0),buffer_pool_manager);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    SetKeyAt(size,key);
    SetValueAt(size,value);
    InternalPage *temp;
    temp = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
    temp->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value,true);
    SetSize(size+1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int size = GetSize();
    recipient->SetKeyAt(0,middle_key);
    recipient->CopyFirstFrom(KeyAt(size-1),ValueAt(size-1),buffer_pool_manager);
    SetSize(size-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(GenericKey *key,const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    int i;
    int size = GetSize();
    GenericKey *temp1;
    page_id_t temp2;
    for(i = (size-1);i >= 0;i--){
        temp1 = KeyAt(i);
        temp2 = ValueAt(i);
        SetKeyAt(i+1,temp1);
        SetValueAt(i+1,temp2);
    }
    SetValueAt(0,value);
    SetKeyAt(0,key);
    SetSize(size+1);
    InternalPage *temp;
    temp = reinterpret_cast<InternalPage *>(buffer_pool_manager->FetchPage(value));
    temp->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value,true);
}