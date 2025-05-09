#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t len = name_.size();
  uint32_t size = 0;
  memcpy(buf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
  size += sizeof(uint32_t);
  memcpy(buf + size, &len, sizeof(uint32_t));
  size += sizeof(uint32_t);
  memcpy(buf + size, name_.data(), len);
  size += len;
  memcpy(buf + size, &type_, sizeof(type_));
  size += sizeof(type_);
  memcpy(buf + size, &len_, sizeof(uint32_t));
  size += sizeof(uint32_t);
  memcpy(buf + size, &table_ind_, sizeof(uint32_t));
  size += sizeof(uint32_t);
  memcpy(buf + size, &nullable_, sizeof(nullable_));
  size += sizeof(nullable_);
  memcpy(buf + size, &unique_, sizeof(unique_));
  size += sizeof(unique_);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t) + sizeof(uint32_t) + name_.size() + sizeof(type_) + sizeof(uint32_t) + sizeof(nullable_) +
           sizeof(unique_);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t size = 0;
  uint32_t magic_num;
  magic_num = MACH_READ_UINT32(buf);
  if(magic_num != COLUMN_MAGIC_NUM){
    column = nullptr;
    return 0;
  }
  size += sizeof(uint32_t);
  uint32_t len = MACH_READ_UINT32(buf + size);
  size += sizeof(uint32_t);
  std::string name(buf + size, len);
  size += len;
  TypeId type = MACH_READ_FROM(TypeId, buf + size);
  size += sizeof(type);
  uint32_t len_ = MACH_READ_UINT32(buf + size);
  size += sizeof(uint32_t);
  uint32_t table_ind = MACH_READ_UINT32(buf + size);
  size += sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool, buf + size);
  size += sizeof(nullable);
  bool unique = MACH_READ_FROM(bool, buf + size);
  size += sizeof(unique);
  column = new Column(name, type, len_, table_ind, nullable, unique);
  return size;
}
