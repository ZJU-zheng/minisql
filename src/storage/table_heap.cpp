#include "storage/table_heap.h"


bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
    page_id_t cur_page_id = first_page_id_,pre_page_id=INVALID_PAGE_ID;
    while(cur_page_id != INVALID_PAGE_ID){
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
        if(page->InsertTuple(row, schema_, txn, lock_manager_,log_manager_)){
            buffer_pool_manager_->UnpinPage(cur_page_id,true);
            return true;
        }
        buffer_pool_manager_->UnpinPage(cur_page_id,false);
        pre_page_id = cur_page_id;
        cur_page_id = page->GetNextPageId();
    }
    page_id_t new_page_id;
    auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
    new_page->Init(new_page_id, pre_page_id, log_manager_, txn);
    new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
    buffer_pool_manager_->UnpinPage(new_page_id,true);
    auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pre_page_id));
    pre_page->SetNextPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(pre_page_id,true);
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}


bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
    page_id_t page_id = rid.GetPageId();
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page == nullptr){
        LOG(ERROR)<<"the buffer pool is full and no space to replace"<<std::endl;
        return false;
    }
    Row *old_row = new Row(rid);
    if(page->UpdateTuple(row, old_row,schema_, txn,lock_manager_, log_manager_)){
        buffer_pool_manager_->UnpinPage(page_id,true);
        delete old_row;
        return true;
    }
    buffer_pool_manager_->UnpinPage(page_id,true);
    delete old_row;
    return false;
}


void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    page->WLatch();
    page->ApplyDelete(rid,txn, nullptr);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}


bool TableHeap::GetTuple(Row *row, Transaction *txn) {
    page_id_t page_id = row->GetRowId().GetPageId();
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if(page == nullptr){
        LOG(ERROR)<<"the buffer pool is full and no space to replace"<<std::endl;
        return false;
    }
    if(page->GetTuple(row,schema_,txn,lock_manager_)){
        if(buffer_pool_manager_->UnpinPage(page_id,false))
            return true;
        LOG(WARNING)<<"unknown mistake"<<std::endl;
        return false;
    }
    LOG(WARNING)<<"get tuple failed"<<std::endl;
    return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}


TableIterator TableHeap::Begin(Transaction *txn) {
    page_id_t cur_page_id = first_page_id_;
    RowId first_rid;
    while(cur_page_id != INVALID_PAGE_ID){
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
        if(page->GetFirstTupleRid(&first_rid)){
            buffer_pool_manager_->UnpinPage(cur_page_id,false);
            Row *row = new Row(first_rid);
            GetTuple(row, nullptr);
            return TableIterator(row->GetRowId(),this);
        }
        buffer_pool_manager_->UnpinPage(cur_page_id,false);
        cur_page_id = page->GetNextPageId();
    }
    LOG(WARNING)<<"1"<<std::endl;
    return End();
}


TableIterator TableHeap::End() {
    return TableIterator(RowId(INVALID_PAGE_ID,0), nullptr);
}
