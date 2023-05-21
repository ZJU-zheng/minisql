#include "record/schema.h"


uint32_t Schema::SerializeTo(char *buf) const {
    uint32_t SerializedSize = 0,temp;
    memcpy(buf, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    temp = GetColumnCount();
    memcpy(buf+SerializedSize, &temp, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    auto ite = columns_.begin();
    for(;ite!=columns_.end();ite++){
        temp = (*ite)->SerializeTo(buf+SerializedSize);
        SerializedSize += temp;
    }
    memcpy(buf+SerializedSize, &is_manage_, sizeof(bool));
    SerializedSize += sizeof(bool);
    return SerializedSize;
}

uint32_t Schema::GetSerializedSize() const {
    uint32_t num = 0;
    auto ite = columns_.begin();
    for(;ite!=columns_.end();ite++){
        num += (*ite)->GetSerializedSize();
    }
    return 2*sizeof(uint32_t) + sizeof(bool) + num;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
    uint32_t SCHEMA_MAGIC_NUM_READ = 0;
    uint32_t SerializedSize = 0;
    uint32_t column_num,i;
    bool is_manage;
    std::vector<Column *> columns;
    memcpy(&SCHEMA_MAGIC_NUM_READ, buf, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    if(SCHEMA_MAGIC_NUM_READ != SCHEMA_MAGIC_NUM)
        LOG(ERROR) << "DeserializeFrom function in schema is wrong" << std::endl;
    memcpy(&column_num, buf+SerializedSize, sizeof(uint32_t));
    SerializedSize += sizeof(uint32_t);
    for(i=0;i<column_num;i++){
        Column *temp = nullptr;
        SerializedSize += Column::DeserializeFrom(buf + SerializedSize, temp);
        columns.push_back(temp);
        temp = nullptr;
    }
    memcpy(&is_manage,buf+SerializedSize,sizeof(bool));
    SerializedSize += sizeof(bool);
    schema = new Schema(columns,is_manage);
    return SerializedSize;
}