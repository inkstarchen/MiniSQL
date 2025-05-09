#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t size = 0;
  memcpy(buf, &SCHEMA_MAGIC_NUM, sizeof(SCHEMA_MAGIC_NUM));
  size += sizeof(SCHEMA_MAGIC_NUM);
  uint32_t column_size = GetColumnCount();
  memcpy(buf+size, &column_size, sizeof(uint32_t));
  size += sizeof(uint32_t);
  for (auto &column : columns_) {
    size += column->SerializeTo(buf + size);
  }
  memcpy(buf+size, &is_manage_, sizeof(is_manage_));
  size += sizeof(is_manage_);
  return size;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(SCHEMA_MAGIC_NUM);
  size += sizeof(uint32_t);
  for (auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  size += sizeof(is_manage_);
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t size = 0;
  uint32_t MAGIC_NUM = MACH_READ_UINT32(buf);
  if(MAGIC_NUM != SCHEMA_MAGIC_NUM){
    schema = nullptr;
    return 0;
  }
  std::vector<Column *> columns;
  size += sizeof(SCHEMA_MAGIC_NUM);
  uint32_t column_size = MACH_READ_UINT32(buf + size);
  size += sizeof(uint32_t);
  for(uint32_t i = 0 ; i < column_size; i++){
    Column *column_new = nullptr;
    size += column_new->DeserializeFrom(buf + size, column_new);
    columns.push_back(column_new);
  }
  bool is_manage = MACH_READ_FROM(bool, buf + size);
  size += sizeof(is_manage);
  schema = new Schema(columns, is_manage);
  return size;
}