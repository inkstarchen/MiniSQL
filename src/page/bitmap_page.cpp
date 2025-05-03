#include "page/bitmap_page.h"

#include "glog/logging.h"

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (next_free_page_ >= GetMaxSupportedSize()) {
    page_offset = GetMaxSupportedSize();     
    return false;
  }
  size_t byte_index = next_free_page_ >> 3, bit_index = next_free_page_ & 0x07;
  page_allocated_ += 1;
  bytes[byte_index] |= (1 << bit_index);
  page_offset = next_free_page_;
  for (;;) {
    if (bytes[byte_index] != 0xFF) {
      for (int i = 0; i < 8; i++) {
        if ((bytes[byte_index] & (1 << i)) == 0) {
          next_free_page_ = (byte_index << 3) + i;
          break;
        }
      }
      break;
    }
    byte_index++;
    if (byte_index >= (GetMaxSupportedSize() >> 3)) {
      next_free_page_ = GetMaxSupportedSize();
      break;
    }
  }
  return true;
}


template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset < 0 || page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  int byte_index = page_offset >> 3, bit_index = page_offset & 0x07;
  if (!(bytes[byte_index] & (1 << bit_index))) {
    return false;
  }
  bytes[byte_index] &= ~(1 << bit_index);
  page_allocated_ -= 1;
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset < 0 || page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  int byte_index = page_offset >> 3, bit_index = page_offset & 0x07;
  return ~(bytes[byte_index] & (1 << bit_index));
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ~(bytes[byte_index] & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;