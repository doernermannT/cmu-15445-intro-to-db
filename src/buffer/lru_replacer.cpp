//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : queue_size_(0), queue_frame_({}) {}

LRUReplacer::~LRUReplacer() = default;

void LRUReplacer::Unpin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> lock(latch_);  
  // Check if the frame already in queue
  auto frame_pos = std::find(queue_frame_.begin(), queue_frame_.end(), frame_id);
  if (frame_pos == queue_frame_.end()) {
    queue_frame_.push_back(frame_id);
    queue_size_++;
  }
}

size_t LRUReplacer::Size() { return queue_size_; }

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  const std::lock_guard<std::mutex> lock(latch_);  
  // If the replacer is empty
  if (queue_size_ == 0) {
    *frame_id = -1;
    return false;
  }

  // deque and update size
  *frame_id = queue_frame_.front();
  queue_frame_.pop_front();
  queue_size_--;

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  const std::lock_guard<std::mutex> lock(latch_);  
  auto frame_pos = std::find(queue_frame_.begin(), queue_frame_.end(), frame_id);

  if (frame_pos != queue_frame_.end()) {
    queue_size_--;
    queue_frame_.erase(frame_pos);
  }
}

}  // namespace bustub
