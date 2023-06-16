#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}


Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    if(page_id == INVALID_PAGE_ID){
    LOG(WARNING)<<"invalid page id"<<std::endl;
      return nullptr;
    }
    frame_id_t frame_id_new;
    if(page_table_.find(page_id) == page_table_.end()){
        if((free_list_.size()==0) && (replacer_->Size()==0))
            return nullptr;
        if(free_list_.size()!=0){
            frame_id_new = free_list_.front();
            free_list_.pop_front();
        }
        else{
            if(!replacer_->Victim(&frame_id_new))
                LOG(WARNING) << "Unknown mistake" << std::endl;
        }
        if(pages_[frame_id_new].is_dirty_){
            disk_manager_->WritePage(pages_[frame_id_new].page_id_,pages_[frame_id_new].data_);
            pages_[frame_id_new].is_dirty_ = false;
        }
        page_table_.erase(pages_[frame_id_new].page_id_);
        page_table_.emplace(page_id,frame_id_new);
        disk_manager_->ReadPage(page_id,pages_[frame_id_new].data_);
        pages_[frame_id_new].is_dirty_ = false;
        pages_[frame_id_new].pin_count_ = 1;
        pages_[frame_id_new].page_id_ = page_id;
        return &pages_[frame_id_new];
    }
    auto ite = page_table_.find(page_id);
    Page *page = &pages_[ite->second];
    replacer_->Pin(ite->second);
    page->pin_count_++;
    return page;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
    int flag = 1;
    frame_id_t frame_id_new;
    if((free_list_.size()==0) && (replacer_->Size()==0))
        return nullptr;
    if(free_list_.size()!=0){
        frame_id_new = free_list_.front();
        free_list_.pop_front();
    }
    else{
        if(!replacer_->Victim(&frame_id_new))
            LOG(WARNING) << "Unknown mistake" << std::endl;
        flag = 0;
    }
    Page * page = &pages_[frame_id_new];
    if((page->is_dirty_) && (flag == 0)){
        disk_manager_->WritePage(page->page_id_,page->data_);
        page->is_dirty_ = false;
    }
    if(flag == 0){
        page_table_.erase(page->page_id_);
    }
    page->pin_count_ = 1;
    page->ResetMemory();
    page_id = AllocatePage();
    page->page_id_ = page_id;
    page_table_.emplace(page_id,frame_id_new);
    return page;
}


bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
    if(page_id == INVALID_PAGE_ID)
        return true;
    if(page_table_.find(page_id) == page_table_.end())
        return true;
    auto ite = page_table_.find(page_id);
    if(pages_[ite->second].pin_count_ != 0){
        LOG(WARNING) << "Someone is using the page" << std::endl;
        return false;
    }
    if(pages_[ite->second].is_dirty_)
        FlushPage(pages_[ite->second].page_id_);
    DeallocatePage(page_id);
    pages_[ite->second].page_id_ = INVALID_PAGE_ID;
    pages_[ite->second].ResetMemory();
    page_table_.erase(page_id);
    free_list_.push_back(ite->second);
    return true;
}


bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    if(page_id == INVALID_PAGE_ID){
        return false;
    }
    if(page_table_.find(page_id) == page_table_.end())
        return false;
    auto ite = page_table_.find(page_id);
    if(pages_[ite->second].pin_count_==0){
        LOG(WARNING) << "this page is already unpin" << std::endl;
        return false;
    }
    pages_[ite->second].pin_count_--;
    if(pages_[ite->second].pin_count_==0)
        replacer_->Unpin(ite->second);
    if(is_dirty)
        pages_[ite->second].is_dirty_ = is_dirty;
    return true;
}


bool BufferPoolManager::FlushPage(page_id_t page_id) {
    if(page_id == INVALID_PAGE_ID)
        return false;
    if(page_table_.find(page_id) == page_table_.end())
        return false;
    auto ite = page_table_.find(page_id);
    disk_manager_->WritePage(page_id,pages_[ite->second].data_);
    pages_[ite->second].is_dirty_ = false;
    return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}