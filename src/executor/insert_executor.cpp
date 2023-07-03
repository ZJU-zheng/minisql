//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  flag=0;
  child_executor_->Init();
  string table_name = plan_->GetTableName();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_name,indexes);
  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if(flag==1){
    return false;
  }
  int count = 0;//记录受影响的行数
  Row row_to_ins;
  RowId rowid_to_ins;
  vector<Field> values_;
  values_.clear();
  while(child_executor_->Next(&row_to_ins, &rowid_to_ins)){
    table_info->GetTableHeap()->InsertTuple(row_to_ins, nullptr);
//    for(auto index:indexes){
//      for(int i = 0;i < index->GetIndexKeySchema()->GetColumnCount();i++){
//        for(int j = 0;j < table_info->GetSchema()->GetColumnCount();j++){
//          if(index->GetIndexKeySchema()->GetColumn(i)->GetName() == table_info->GetSchema()->GetColumn(j)->GetName()){
//            values_.push_back(*(row_to_ins.GetField(j)));
//          }
//        }
//      }
//      index->GetIndex()->InsertEntry(Row(values_),rowid_to_ins, nullptr);
//    }
    for(auto index:indexes){
            vector<Field> key_inf;
            for (auto col:index->GetIndexKeySchema()->GetColumns()){
              uint32_t col_index;
              table_info->GetSchema()->GetColumnIndex(col->GetName(),col_index);
              key_inf.push_back(*(row_to_ins.GetField(col_index)));
            }
            Row key_row(key_inf);
            key_row.SetRowId(row_to_ins.GetRowId());
            rowid_to_ins = row_to_ins.GetRowId();
            auto status=index->GetIndex()->InsertEntry(key_row,rowid_to_ins, nullptr);
            if (status != DB_SUCCESS) {
              table_info->GetTableHeap()->MarkDelete(row_to_ins.GetRowId(), nullptr);
              LOG(ERROR) << "Duplicate primary key or unique column.";
              return false;
            }
          }
    count++;
  }
  std::vector<Field> values;
  values.clear();
  values.emplace_back(Field(kTypeInt,count));
  (*row) = Row(values);
  flag = 1;
  return true;
}