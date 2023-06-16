//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


void UpdateExecutor::Init() {
  string table_name = plan_->GetTableName();
  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
  flag=0;
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if(flag==1){
    return false;
  }
  std::vector<IndexInfo *> indexes;
  Row old_row;
  Row new_row;
  RowId rowid;
  vector<Field> values_;
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes);
  while(child_executor_->Next(&old_row, &rowid)){
    old_row.SetRowId(rowid);
    new_row = GenerateUpdatedTuple(old_row);
    new_row.SetRowId(rowid);
    if(!table_info->GetTableHeap()->UpdateTuple(new_row,rowid, nullptr)){
      return false;
    }
  }
  for(auto index:indexes){
    index->GetIndex()->RemoveEntry(old_row,rowid, nullptr);
    index->GetIndex()->InsertEntry(new_row,rowid, nullptr);
  }
  flag = 1;
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  auto update_attr = plan_->GetUpdateAttr();
  uint32_t count = table_info->GetSchema()->GetColumnCount();
  std::vector<Field> values;
  values.clear();
  for(uint32_t i=0;i<count;i++){
    if(update_attr.find(i) == update_attr.end()){
      values.push_back(*(src_row.GetField(i)));
    }
    else{
      Field temp = update_attr[i]->Evaluate(&src_row);
      values.push_back(temp);
    }
  }
  return Row(values);
}