#include "page/bitmap_page.h"

#include "glog/logging.h"


template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    if(page_allocated_ == 8 * MAX_CHARS){
        //LOG(WARNING) << "there is no page available" <<std::endl;
        return false;
    }
    bytes[next_free_page_/8] |= (1<<(7-(next_free_page_%8)));
    page_allocated_++;
    page_offset = next_free_page_;
    next_free_page_ = 8 * MAX_CHARS;
    for(uint32_t i = (page_offset+1);i < 8 * MAX_CHARS;i++){
        if((bytes[i/8]&(1<<(7-(i%8)))) == 0){
            next_free_page_ = i;
            break;
        }
    }
    return true;
}


template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    if(IsPageFree(page_offset)){
        //LOG(WARNING) << "this page is already available" <<std::endl;
        return false;
    }
    bytes[page_offset/8] &= ~(1<<(7-(page_offset%8)));
    page_allocated_--;
    if(next_free_page_ > page_offset)
        next_free_page_ = page_offset;
    return true;
}


template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    if(bytes[byte_index]&(1<<(7-bit_index)))
        return false;
    return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;