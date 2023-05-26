#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"


TableIterator::TableIterator(Row *row,TableHeap *tableheap) {
    ite_row = row;
    ite_tableheap = tableheap;
}

TableIterator::TableIterator(const TableIterator &other) {
    this->ite_row = other.ite_row;
    this->ite_tableheap = other.ite_tableheap;
}

TableIterator::~TableIterator() {
    if(ite_row != nullptr)
        delete ite_row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
    return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return false;
}

const Row &TableIterator::operator*() {
    return *ite_row;
}

Row *TableIterator::operator->() {
    return ite_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  ASSERT(false, "Not implemented yet.");
}

// ++iter
TableIterator &TableIterator::operator++() {
    auto page = reinterpret_cast<TablePage *>(ite_tableheap->buffer_pool_manager_->FetchPage(ite_row->GetRowId().GetPageId()));
    if(page == nullptr){
        LOG(ERROR)<<"unknown mistake"<<std::endl;
    }
    RowId next_rowid;
    if(page->GetNextTupleRid(ite_row->GetRowId(),&next_rowid)){
        ite_row->SetRowId(next_rowid);
        ite_tableheap->GetTuple(ite_row, nullptr);
        ite_tableheap->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        return *this;
    }
    ite_tableheap->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    while(page->GetNextPageId() != INVALID_PAGE_ID){
        page = reinterpret_cast<TablePage *>(ite_tableheap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
        if(page->GetFirstTupleRid(&next_rowid)){
            ite_row->SetRowId(next_rowid);
            ite_tableheap->GetTuple(ite_row, nullptr);
            ite_tableheap->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
            return *this;
        }
    }
    ite_row = nullptr;
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
    Row *row = new Row(*ite_row);
    ++(*this);
    return TableIterator(row,ite_tableheap);
}
