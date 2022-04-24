//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      starting_index_(0)
{
  // Allocate and create individual BufferPoolManagerInstances
    for (size_t i = 0; i < num_instances; i++) {
      auto *bpm = new BufferPoolManagerInstance(pool_size, num_instances, i,
                                                disk_manager, log_manager);
      bpmis_.push_back(bpm);
    }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  for (size_t i = 0; i < num_instances_; i++) {
      delete bpmis_[i];
  }
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return (num_instances_ * pool_size_);
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use
  // this method in your other methods.
  return bpmis_[page_id % num_instances_];
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->FlushPage(page_id);  

}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner
  // from the underlying BufferPoolManagerInstances
  // 1.  From a starting index
  // of the BPMIs, call NewPageImpl until either 1) success and return 2)
  // looped around to starting index and return nullptr
  // 2.  Bump the starting index (mod number of instances) to start search at a
  // different BPMI each time this function is called
  const std::lock_guard<std::mutex> lock(latch_);
  size_t i = starting_index_;

  // Create a new page in every bpmi in a round robin manner
  do {
    Page *page = bpmis_[i]->NewPage(page_id);

    // No free slots and all frames are pinned in the current bpmi
    if (page == nullptr) {
      i = (i + 1) % num_instances_;
    } else {  // Success
      starting_index_ = (i + 1) % num_instances_;
      return page;
    }
  } while (i != starting_index_);

  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolManager(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances_; i++) {
    bpmis_[i]->FlushAllPages();
  }
}

}  // namespace bustub
