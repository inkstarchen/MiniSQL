#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  MACH_WRITE_INT32(buf, rid_.GetPageId());
  size += sizeof(int32_t);
  MACH_WRITE_UINT32(buf + size, rid_.GetSlotNum());
  size += sizeof(uint32_t);
  for(Field * field :fields_){
    TypeId type = field->GetTypeId();
    bool is_null = field->IsNull();
    memcpy(buf + size, &is_null, sizeof(bool));
    size += sizeof(bool);
    memcpy(buf + size, &type, sizeof(TypeId));
    size += sizeof(TypeId);
    size += field->SerializeTo(buf + size);
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t size = 0;
  int32_t page_id = MACH_READ_INT32(buf);
  size += sizeof(int32_t);
  uint32_t slot_num = MACH_READ_UINT32(buf + size);
  size += sizeof(uint32_t);
  rid_.Set(page_id,slot_num);
  for(uint32_t i = 0; i < schema->GetColumnCount(); i++){
    bool is_null = MACH_READ_FROM(bool, buf + size);
    size += sizeof(bool);
    TypeId type = MACH_READ_FROM(TypeId ,buf + size);
    size += sizeof(TypeId);
    Field ** field = nullptr;
    size += (*field)->DeserializeFrom(buf + size, type, field, is_null);
    fields_.emplace_back(*field);
  }
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  size += sizeof(int32_t) + sizeof(uint32_t);
  for(Field * field : fields_){
    size += sizeof(bool) + sizeof(TypeId);
    size += field->GetSerializedSize();
  }

  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
