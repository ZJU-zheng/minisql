#include "record/row.h"


uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    uint32_t SerializedSize = 0,temp,count=0;
    uint32_t field_count = GetFieldCount();
    memcpy(buf, &field_count, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    if(field_count == 0)
        return SerializedSize;
    //store null bitmap
    uint32_t byte_size = ceil(field_count*1.0/8);
    char *null_bitmaps = new char[byte_size];
    memset(null_bitmaps,0,byte_size);
    auto ite = fields_.begin();
    for(;ite!=fields_.end();ite++){
        if((*ite)->IsNull() != true)//1 represent it is not null
            null_bitmaps[count/8] |= (1<<(7-(count%8)));
        count++;
    }
    memcpy(buf+SerializedSize, null_bitmaps, byte_size*sizeof(char));
    SerializedSize += byte_size*sizeof(char);
    ite = fields_.begin();
    for(;ite!=fields_.end();ite++){
        temp = (*ite)->SerializeTo(buf+SerializedSize);
        SerializedSize += temp;
    }
    delete []null_bitmaps;
    return SerializedSize;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
    fields_.resize(0);
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    uint32_t SerializedSize = 0;
    uint32_t num = 0,i;
    Field *temp;
    TypeId type;
    memcpy(&num, buf, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    if(num == 0)
        return SerializedSize;
    uint32_t byte_size = ceil(num*1.0/8);
    char *null_bitmaps = new char[byte_size];
    memcpy(null_bitmaps, buf+SerializedSize, byte_size*sizeof(char));
    SerializedSize += byte_size*sizeof(char);
    for(i=0;i < num;i++){
        type = schema->GetColumn(i)->GetType();
        if((null_bitmaps[i/8]&(1<<(7-(i%8)))) !=0){//is not null
            if(type == TypeId::kTypeInt){
                int32_t row_int = 0;
                memcpy(&row_int, buf+SerializedSize, sizeof(int));
                SerializedSize += sizeof(int);
                temp = new Field(type,row_int);
            }
            else if(type == TypeId::kTypeFloat){
                float row_float = 0;
                memcpy(&row_float, buf+SerializedSize, sizeof(float));
                SerializedSize += sizeof(float);
                temp = new Field(type,row_float);
            }
            else{
                uint32_t len = 0;
                memcpy(&len, buf+SerializedSize, sizeof(uint32_t));
                SerializedSize += sizeof(uint32_t);
                char *value = new char[len];
                memcpy(value, buf+SerializedSize, len);
                SerializedSize += len;
                temp = new Field(type,value,len,true);
                delete []value;
            }
        }
        else{
            temp = new Field(type);
        }
        fields_.push_back(temp);
    }
    delete []null_bitmaps;
    return SerializedSize;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    uint32_t num = 0;
    auto ite = fields_.begin();
    for(;ite!=fields_.end();ite++)
        num += (*ite)->GetSerializedSize();
    return sizeof(uint32_t)+ceil(GetFieldCount()*1.0/8)+num;
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
