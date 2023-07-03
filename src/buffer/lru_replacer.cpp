#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages):cache(num_pages, victims.end()){
  num_pages_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(victims.empty()){
    return false;
  }
  (*frame_id) = victims.back();
  cache[*frame_id] = victims.end();
  victims.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  auto temp = cache[frame_id];
  if (temp != victims.end()) {
    victims.erase(temp);
    cache[frame_id] = victims.end();
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (victims.size() >= num_pages_ || cache[frame_id] != victims.end()) {
    return;
  }
  victims.push_front(frame_id);
  cache[frame_id] = victims.begin();
}

size_t LRUReplacer::Size(){
  return victims.size();
}