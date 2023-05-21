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


uint32_t Column::SerializeTo(char *buf) const {
    uint32_t SerializedSize = 0;
    memcpy(buf, &COLUMN_MAGIC_NUM, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    uint32_t temp = name_.length();
    memcpy(buf+SerializedSize, &temp, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    const char *name_char = name_.c_str();
    memcpy(buf+SerializedSize,name_char,name_.length());
    SerializedSize += name_.length();
    memcpy(buf+SerializedSize,&type_,sizeof(type_));
    SerializedSize += sizeof(type_);
    memcpy(buf+SerializedSize,&len_,sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    memcpy(buf+SerializedSize,&table_ind_,sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    memcpy(buf+SerializedSize,&nullable_,sizeof(bool));
    SerializedSize += sizeof(bool);
    memcpy(buf+SerializedSize,&unique_,sizeof(bool));
    SerializedSize += sizeof(bool);
    return SerializedSize;
}


uint32_t Column::GetSerializedSize() const {
    return 4*sizeof(uint32_t) + name_.length() + sizeof(type_) + 2*sizeof(bool);
}


uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
    uint32_t COLUMN_MAGIC_NUM_Read = 0;
    TypeId type;
    uint32_t len;
    uint32_t table_ind;
    bool nullable,unique;
    uint32_t SerializedSize = 0;
    memcpy(&COLUMN_MAGIC_NUM_Read, buf, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    if(COLUMN_MAGIC_NUM_Read != COLUMN_MAGIC_NUM)
        LOG(ERROR) << "DeserializeFrom function in column is wrong" << std::endl;
    uint32_t temp;
    memcpy(&temp, buf+SerializedSize, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    char * name_char = new char[temp+1];
    memcpy(name_char,buf+SerializedSize,temp);
    name_char[temp] = '\0';
    std::string name(name_char,temp);
    SerializedSize += temp;
    memcpy(&type,buf+SerializedSize,sizeof(type));
    SerializedSize += sizeof(type);
    memcpy(&len,buf+SerializedSize,sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    memcpy(&table_ind,buf+SerializedSize,sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    memcpy(&nullable,buf+SerializedSize,sizeof(bool));
    SerializedSize += sizeof(bool);
    memcpy(&unique,buf+SerializedSize,sizeof(bool));
    SerializedSize += sizeof(bool);
    //
    if(type == TypeId::kTypeChar)
        column = new Column(name, type, len, table_ind, nullable, unique);
    else
        column = new Column(name, type, table_ind, nullable, unique);
    delete []name_char;
    return SerializedSize;
}
