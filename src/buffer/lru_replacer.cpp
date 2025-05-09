#include "buffer/lru_replacer.h"
#include <algorithm>
using namespace std;

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // for (auto it : lru_list) {
  //   printf("%d\n",it);
  // }
  if (lru_list.empty()) {
    return false;
  }
  *frame_id = lru_list.front();
  lru_list.pop_front();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto it = find(lru_list.begin(), lru_list.end(), frame_id);
  if (find(lru_list.begin(), lru_list.end(), frame_id) != lru_list.end()) {
    lru_list.erase(it);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (find(lru_list.begin(), lru_list.end(), frame_id) == lru_list.end()) {
    lru_list.push_back(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}