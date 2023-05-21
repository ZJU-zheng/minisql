#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188), Field(TypeId::kTypeInt, -65537), Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),   Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    delete df;
    df = nullptr;
  }
}

TEST(TupleTest, RowTest) {
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}
Column column_test[]{
        Column("a1_ test", TypeId::kTypeInt, 3, true, true),
        Column("a2", TypeId::kTypeChar, 1, 0, true, true),
        Column("a3", TypeId::kTypeFloat, 4, false, false),
        Column("a4", TypeId::kTypeChar, 255, 99, false, false),
};
TEST(TupleTest, ColumnTest) {
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    // Serialize phase
    char *p = buffer;
    for (int i = 0; i < 4; i++)
        p += column_test[i].SerializeTo(p);
    // Deserialize phase
    uint32_t ofs = 0;
    Column *df = nullptr;
    ofs += Column::DeserializeFrom(buffer + ofs, df);
    ASSERT_EQ("a1_ test", df->GetName());
    ASSERT_EQ(TypeId::kTypeInt, df->GetType());
    ASSERT_EQ(3, df->GetTableInd());
    ASSERT_EQ(true, df->IsNullable());
    ASSERT_EQ(true, df->IsUnique());
    ASSERT_EQ(ofs,df->GetSerializedSize());
    delete df;
    df = nullptr;
    ofs += Column::DeserializeFrom(buffer + ofs,df);
    ASSERT_EQ("a2", df->GetName());
    ASSERT_EQ(TypeId::kTypeChar, df->GetType());
    ASSERT_EQ(1, df->GetLength());
    ASSERT_EQ(0, df->GetTableInd());
    ASSERT_EQ(true, df->IsNullable());
    ASSERT_EQ(true, df->IsUnique());
    delete df;
    df = nullptr;
    ofs += Column::DeserializeFrom(buffer + ofs, df);
    ASSERT_EQ("a3", df->GetName());
    ASSERT_EQ(TypeId::kTypeFloat, df->GetType());
    ASSERT_EQ(4, df->GetTableInd());
    ASSERT_EQ(false, df->IsNullable());
    ASSERT_EQ(false, df->IsUnique());
    delete df;
    df = nullptr;
    ofs += Column::DeserializeFrom(buffer + ofs,df);
    ASSERT_EQ("a4", df->GetName());
    ASSERT_EQ(TypeId::kTypeChar, df->GetType());
    ASSERT_EQ(255, df->GetLength());
    ASSERT_EQ(99, df->GetTableInd());
    ASSERT_EQ(false, df->IsNullable());
    ASSERT_EQ(false, df->IsUnique());
    delete df;
    df = nullptr;
}
TEST(TupleTest, SchemaTest) {
    // create schema
    std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                     new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account", TypeId::kTypeFloat, 2, true, false)};
    auto schema = new Schema(columns,true);
    char buffer[PAGE_SIZE];
    memset(buffer, 0, sizeof(buffer));
    // Serialize phase
    char *p = buffer;
    p += schema->SerializeTo(p);
    uint32_t ofs = 0;
    Schema *df= nullptr;
    ofs += Schema::DeserializeFrom(buffer, df);
    ASSERT_EQ(3,df->GetColumnCount());
    ASSERT_EQ("id",df->GetColumn(0)->GetName());
    ASSERT_EQ(TypeId::kTypeInt,df->GetColumn(0)->GetType());
    ASSERT_EQ(0,df->GetColumn(0)->GetTableInd());
    ASSERT_EQ(false,df->GetColumn(0)->IsNullable());
    ASSERT_EQ(false,df->GetColumn(0)->IsUnique());
    ASSERT_EQ("name",df->GetColumn(1)->GetName());
    ASSERT_EQ(TypeId::kTypeChar,df->GetColumn(1)->GetType());
    ASSERT_EQ(64,df->GetColumn(1)->GetLength());
    ASSERT_EQ(1,df->GetColumn(1)->GetTableInd());
    ASSERT_EQ(true,df->GetColumn(1)->IsNullable());
    ASSERT_EQ(false,df->GetColumn(1)->IsUnique());
    ASSERT_EQ("account",df->GetColumn(2)->GetName());
    ASSERT_EQ(TypeId::kTypeFloat,df->GetColumn(2)->GetType());
    ASSERT_EQ(2,df->GetColumn(2)->GetTableInd());
    ASSERT_EQ(true,df->GetColumn(2)->IsNullable());
    ASSERT_EQ(false,df->GetColumn(2)->IsUnique());
    ASSERT_EQ(ofs,df->GetSerializedSize());
    delete df;
    df = nullptr;
}