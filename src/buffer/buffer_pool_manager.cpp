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

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    Page *page = &page[it->second];
    page->pin_count_ ++;
    return page;
  }
  else {
    frame_id_t frame_id;
    if (replacer_->Victim(&frame_id)) {
      Page *victim = &pages_[frame_id];
      if (victim->IsDirty()) {
        disk_manager_->WritePage(victim->page_id_, victim->data_);
      }
      page_table_.erase(victim->page_id_);
      Page *new_page = &pages_[frame_id];
      new_page->page_id_ = page_id;
      new_page->pin_count_ = 1;
      new_page->is_dirty_ = false;
      disk_manager_->ReadPage(page_id, new_page->data_);
      page_table_[page_id] = frame_id;
      return new_page;
    }
    else return nullptr;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  bool flag = false;
  // printf("\n");
  for (size_t i = 0; i < pool_size_; i++) {
    // printf("%d %d\n", i, pages_[i].pin_count_);
    if (!pages_[i].pin_count_) {
      flag = true;
    }
  }
  // printf("flag = %d\n",flag);
  if (!flag) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  else {
    if (replacer_->Victim(&frame_id)) {
      Page *victim = &pages_[frame_id];
      if (victim->IsDirty()) {
        disk_manager_->WritePage(victim->page_id_, victim->data_);
      }
      page_table_.erase(victim->page_id_);
    }
    else {
      return nullptr;
    }
  }
  Page *new_page = &pages_[frame_id];
  page_id = disk_manager_->AllocatePage();
  new_page->page_id_ = page_id;
  new_page->pin_count_ = 1;
  new_page->is_dirty_ = false;
  memset(new_page->data_, 0, PAGE_SIZE);
  page_table_[page_id] = frame_id;
  return new_page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  else {
    if (pages_[page_id].GetPinCount() != 0) {
      return false;
    }
    else {
      free_list_.push_back(page_table_[page_id]);
      page_table_.erase(page_table_.find(page_id));
      disk_manager_->DeAllocatePage(page_id);
      return true;
    }
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  Page *page = &pages_[it->second];
  page->is_dirty_ = is_dirty;
  if (page->pin_count_ > 0) {
    page->pin_count_ --;
  }
  if (!page->pin_count_) {
    replacer_->Unpin(it->second);
  }
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_id].data_);
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