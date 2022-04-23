//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/config.h"
#include "common/macros.h"

namespace bustub {
BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

/**
 * Flushes the target page to disk.
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  std::scoped_lock lock{latch_};  
  // Cant find the page in the PT or the page ID is invalid
  if (!IsInPageTable(page_id) || (page_id == INVALID_PAGE_ID)) {
    return false;
  }

  // Write back dirty page to disk
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;  // Reset the dirty page
  }
  
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock{latch_};  

  for (const auto &n : page_table_) {
    if (FlushPgImp(n.first)) {
    }
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  frame_id_t new_frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&new_frame_id)) {
    // Then, find one in the replacer using LRU
    page_id_t new_page_id = pages_[new_frame_id].GetPageId();

    // Save the page as a backup in disk
    if (pages_[new_frame_id].IsDirty()) {
        disk_manager_->WritePage(new_page_id, pages_[new_frame_id].GetData());
        pages_[new_frame_id].is_dirty_ = false;  // Reset the dirty page
    }

    page_table_.erase(new_page_id);
  } else {  // free list and all current pages are pinned in buffer pool
    return nullptr;
  }

  page_id_t new_page_id = AllocatePage();

  // Update metadata of the new page
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].pin_count_ = 1;
  pages_[new_frame_id].is_dirty_ = false;
  pages_[new_frame_id].ResetMemory();  // Zero out its actual data for future loading

  // Update the page table
  // page_table_.erase(old_page_id);
  page_table_.insert({new_page_id, new_frame_id});

  // Store (meta)data into the passed-in variable
  *page_id = new_page_id;

  // Missing
  replacer_->Pin(new_frame_id);
  
  return &pages_[new_frame_id];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // const std::lock_guard<std::mutex> lock(latch_);

  return nullptr;

  // // Retrieve the page directly if it's in the page table
  // if (IsInPageTable(page_id)) {
  //   frame_id_t frame_id = page_table_[page_id];
  //   pages_[frame_id].pin_count_ = 1;
  //   return &pages_[frame_id];
  // }

  // // Cant find a victim page because free list is full and
  // // all existing pages are pinned
  // frame_id_t victim_frame_id = GetVictimPage();
  // if (victim_frame_id < 0) {
  //   return nullptr;
  // }
  
  // // Write back to disk and delete it in memory if necessary
  // if (FlushPgImp(victim_frame_id)) {
  // }  // Call flush
  // if (DeletePgImp(victim_frame_id)) {
  // }  // Call delete

  // // const std::lock_guard<std::mutex> lock(latch_);
  // // Update the page table + free list
  // page_table_.insert({page_id, victim_frame_id});
  // free_list_.remove(victim_frame_id);
  // // No need to update the pages_ as it's only a place holder

  // // Update the metadata
  // // Read in page from the disk
  // char page_data[PAGE_SIZE] = {0};              // Initialize empty page data stream
  // disk_manager_->ReadPage(page_id, page_data);  // Read disk page into memory page
  // // Update the page itself after its memory has been zeroed out
  // std::memcpy(pages_[victim_frame_id].GetData(), page_data, PAGE_SIZE);
  // pages_[victim_frame_id].page_id_ = page_id;
  // pages_[victim_frame_id].is_dirty_ = false;
  // pages_[victim_frame_id].pin_count_ = 1;

  // return &pages_[victim_frame_id];
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  const std::lock_guard<std::mutex> lock(latch_);
  
  // Cant find the page in PT
  if (!IsInPageTable(page_id)) {
    return true;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() != 0) {  // The page is pinned
    return false;
  }

  if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
  }
  
  // Update the metadata of the page place holder
  pages_[frame_id].ResetMemory();  // Zero out its actual data for future loading
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;

  free_list_.push_back(frame_id);  // Update the free list
  page_table_.erase(page_id);      // Update the page table

  // NOTICE: since the page is no longer in the BP and written back to disk,
  // we need to remove it also from the replacer
  replacer_->Pin(frame_id);

  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  const std::lock_guard<std::mutex> lock(latch_);

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() <= 0 || !IsInPageTable(page_id)) {  // Already unpinned
    return false;
  }

  // NOTICE: see prompt
  if (is_dirty) {
      pages_[frame_id].is_dirty_ = true;
  }

  // Need to move the frame into the replacer if pin count goes to 0
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].GetPinCount() <= 0) {
      // Move the unpined page to the LRUreplacer
      replacer_->Unpin(frame_id);
  }  

  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

bool BufferPoolManagerInstance::IsInPageTable(page_id_t page_id) {
  return (page_table_.find(page_id) != page_table_.end());
}

}  // namespace bustub
