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

namespace bustub {

    LRUReplacer::LRUReplacer(size_t num_pages) : size(0), queueFrames({}) {}
    
    LRUReplacer::~LRUReplacer() = default;

    void LRUReplacer::Unpin(frame_id_t frame_id) {
      // Check if the frame already in queue
      auto framePos = std::find(queueFrames.begin(), queueFrames.end(), frame_id);
      if (framePos == queueFrames.end()) {
        queueFrames.push_back(frame_id);
        size++;
      }
    }

    size_t LRUReplacer::Size() { return size; }

    bool LRUReplacer::Victim(frame_id_t *frame_id) {
      // If the replacer is empty
      if (size == 0) {
          *frame_id = -1;
          return false;
      }

      // deque and update size
      *frame_id = queueFrames.front();
      queueFrames.pop_front();
      size--;

      return true;
    }

    void LRUReplacer::Pin(frame_id_t frame_id) {
      auto framePos = std::find(queueFrames.begin(), queueFrames.end(), frame_id);

      if (framePos != queueFrames.end()) {
        size--;
        queueFrames.erase(framePos);
      }
    }

}  // namespace bustub
