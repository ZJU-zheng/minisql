//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"



DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_name = plan_->GetTableName();
  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
  table_heap = table_info->GetTableHeap();
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if(flag==1){
    return false;
  }
  std::vector<IndexInfo *> indexes;
  int count = 0;//记录受影响的行数
  Row row_to_del;
  RowId rowid_to_del;
  vector<Field> values_;
  while(child_executor_->Next(&row_to_del, &rowid_to_del)){
    exec_ctx_->GetCatalog()->GetTableIndexes(table_name,indexes);
    table_heap->MarkDelete(rowid_to_del, nullptr);
    for(auto index:indexes){
      index->GetIndex()->RemoveEntry(row_to_del,rowid_to_del, nullptr);
    }
    count++;
  }
  std::vector<Field> values;
  values.emplace_back(Field(kTypeInt,count));
  (*row) = Row(values);
  flag = 1;
  return true;
}