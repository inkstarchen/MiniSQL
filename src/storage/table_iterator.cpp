#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid){
  table_heap_ = table_heap;
  rid_ = rid;
  is_end_ = true;
  txn_ = nullptr;
}
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  table_heap_ = table_heap;
  txn_ = txn;
  rid_ = rid;
}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn,bool is_end) {
  table_heap_ = table_heap;
  txn_ = txn;
  rid_ = rid;
  is_end_ = false;
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_),
      txn_(other.txn_),
      rid_(other.rid_),
      is_end_(other.is_end_),
      current_row_(other.current_row_) {}


TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(table_heap_ == itr.table_heap_ && txn_ == itr.txn_ && rid_ == itr.rid_){
    return true;
  }
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(table_heap_ == itr.table_heap_ && txn_ == itr.txn_ && rid_ == itr.rid_){
    return false;
  }
  return true;
}

const Row &TableIterator::operator*() {
  current_row_ = Row(rid_);
  table_heap_->GetTuple(&current_row_, nullptr);
  return current_row_;
}

Row *TableIterator::operator->() {
  if(!is_end_){
    current_row_ = Row(rid_);
  }else{
    current_row_ = Row(rid_);
    table_heap_->GetTuple(&current_row_, txn_);
  }
  return &current_row_;
}

TableIterator &TableIterator::operator=(const TableIterator &other) noexcept {
  if (this != &other) {
    table_heap_ = other.table_heap_;
    rid_ = other.rid_;
    is_end_ = other.is_end_;
    current_row_ = other.current_row_;
  }
  return *this;
}


// ++iter
TableIterator &TableIterator::operator++() {
  // auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));
  // RowId next_rid;
  // page->GetNextTupleRid(rid_, &next_rid);
  // rid_ = next_rid;
  // if(rid_ == INVALID_ROWID) is_end_ = true;
  // return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  // ASSERT(table_heap_ != nullptr, "table_heap_ is nullptr");
  // TableIterator tmp(*this);  
  // ++(*this);                
  // return tmp;               
}
