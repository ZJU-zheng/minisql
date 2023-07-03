//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),ite(RowId(INVALID_PAGE_ID,0), nullptr),end(RowId(INVALID_PAGE_ID,0), nullptr){
}

void SeqScanExecutor::Init() {
  schema_index.clear();
  table_name = plan_->GetTableName();
  //  LOG(WARNING)<<table_name<<std::endl;
  //  if(plan_->GetPredicate() == nullptr){
  //    select *
  //    LOG(WARNING)<<"empty tree"<<std::endl;
  //  }
  //  else if(plan_->GetPredicate()->GetType() == ExpressionType::LogicExpression){
  //    LOG(WARNING)<<"head:logic"<<std::endl;
  //  }
  //  else if(plan_->GetPredicate()->GetType() == ExpressionType::ComparisonExpression){
  //    LOG(WARNING)<<"head:compare"<<std::endl;
  //  }
  out_schema = plan_->OutputSchema();
  //  LOG(WARNING)<<out_schema->GetColumnCount()<<std::endl;
  //  int i = out_schema->GetColumnCount();
  //  for(i=0;i<out_schema->GetColumnCount();i++){
  //    LOG(WARNING)<<out_schema->GetColumn(i)->GetName()<<std::endl;
  //  }
  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
  ite = table_info->GetTableHeap()->Begin(nullptr);
  end = table_info->GetTableHeap()->End();
  values.reserve(out_schema->GetColumnCount());
  int count1=table_info->GetSchema()->GetColumnCount();
  int count2=out_schema->GetColumnCount();
  int i,j;
  for(i=0;i<count2;i++){
    for(j=0;j<count1;j++){
      if(out_schema->GetColumn(i)->GetName() == table_info->GetSchema()->GetColumn(j)->GetName()){
        schema_index.push_back(j);
        break;
      }
    }
  }
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  //LOG(WARNING)<<ite->GetRowId().GetPageId()<<" "<<ite->GetRowId().GetSlotNum()<<std::endl;
  while(1){
    if(ite == end){
      return false;
    }
    (*rid) = RowId(ite->GetRowId());
    row->SetRowId(*rid);
    table_info->GetTableHeap()->GetTuple(row, nullptr);
    if(plan_->GetPredicate() != nullptr){
      if(plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt,1))){
        ite++;
        int i;
        vector<Field> field;
        field.clear();
        for(i=0;i<out_schema->GetColumnCount();i++){
          field.push_back(*(row->GetField(schema_index[i])));
        }
        (*row) = Row(field);
        //row = new Row(field);
        //(*row) = Row(*(ite));
        row->SetRowId(*rid);
        return true;
      }
      else{
        ite++;
      }
    }
    else{
      ite++;
      return true;
    }
  }
}