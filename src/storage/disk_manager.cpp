#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  uint32_t num_extents = *(reinterpret_cast<uint32_t *>(meta_data_) + 1);
  for (uint32_t i = 0; i < num_extents; i++) {
    uint32_t extent_used_page = *(reinterpret_cast<uint32_t *>(meta_data_) + 2 + i);
    if (extent_used_page < BITMAP_SIZE) {
      page_id_t bitmap_page_id = i * BITMAP_SIZE;
      char bitmap_page[PAGE_SIZE];
      ReadPhysicalPage(bitmap_page_id, bitmap_page);
      BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page);
      uint32_t ofs;
      bitmap->AllocatePage(ofs);
      WritePhysicalPage(bitmap_page_id, bitmap_page);
      *(reinterpret_cast<uint32_t *>(meta_data_)) += 1;
      *(reinterpret_cast<uint32_t *>(meta_data_) + 2 + i) += 1;
      return bitmap_page_id + ofs;
    }
  }
  // if (num_extents < MAX_EXTENT_NUM) {
    // allocate new extent
    // printf("Here!\n");
    page_id_t bitmap_page_id = num_extents * BITMAP_SIZE;
    char bitmap_page[PAGE_SIZE];
    ReadPhysicalPage(bitmap_page_id, bitmap_page);
    BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page);
    uint32_t ofs;
    bitmap->AllocatePage(ofs);
    WritePhysicalPage(bitmap_page_id, bitmap_page);
    *(reinterpret_cast<uint32_t *>(meta_data_)) += 1;
    *(reinterpret_cast<uint32_t *>(meta_data_) + 1) += 1;
    *(reinterpret_cast<uint32_t *>(meta_data_) + 2 + num_extents) = 1;
    // printf("%lx %lx\n", bitmap_page_id, bitmap_page_id + ofs);
    return bitmap_page_id + ofs;
  // }
  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  size_t extent_id = logical_page_id / BITMAP_SIZE, offset = logical_page_id % BITMAP_SIZE;
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1), bitmap_page);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page);
  bitmap->DeAllocatePage(offset);
  WritePhysicalPage(extent_id * (BITMAP_SIZE + 1), bitmap_page);
  *(reinterpret_cast<uint32_t *>(meta_data_)) -= 1;
  *(reinterpret_cast<uint32_t *>(meta_data_) + 2 + extent_id) -= 1;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  if (logical_page_id < 0 || logical_page_id >= MAX_VALID_PAGE_ID) {
    return false;
  }
  size_t extent_id = logical_page_id / BITMAP_SIZE, offset = logical_page_id % BITMAP_SIZE;
  char bitmap_page[PAGE_SIZE];
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1), bitmap_page);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page);
  return bitmap->IsPageFree(offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  size_t extent_id = logical_page_id / BITMAP_SIZE, offset = logical_page_id % BITMAP_SIZE;
  return extent_id * (BITMAP_SIZE + 1) + offset + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}