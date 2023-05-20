#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){
    num_pages_=num_pages;
}

LRUReplacer::~LRUReplacer() = default;


bool LRUReplacer::Victim(frame_id_t *frame_id) {
    if(Size() == 0)
        return false;
    (*frame_id) = lru_list_.front();
    lru_list_.pop_front();
    return true;
}


void LRUReplacer::Pin(frame_id_t frame_id) {
    list<frame_id_t>::iterator ite;
    for(ite = lru_list_.begin();ite != lru_list_.end();ite++){
        if((*ite) == frame_id){
            lru_list_.remove(frame_id);
            break;
        }
    }
}


void LRUReplacer::Unpin(frame_id_t frame_id) {
    uint32_t flag = 0;
    if(Size() < num_pages_){
        list<frame_id_t>::iterator ite;
        for(ite = lru_list_.begin();ite != lru_list_.end();ite++){
            if((*ite) == frame_id){
                flag = 1;
                break;
            }
        }
        if(flag == 0)
            lru_list_.push_back(frame_id);
    }
}


size_t LRUReplacer::Size() {
    return lru_list_.size();
}