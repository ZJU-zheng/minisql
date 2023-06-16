#include "executor/executors/index_scan_executor.h"

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
//  index_infos = plan_->indexes_;
//  string index_name;
//  string table_name = plan_->GetTableName();
//  exec_ctx_->GetCatalog()->GetTable(table_name,table_info);
//  for(auto index_info:index_infos){
//    index_name = index_info->GetIndexName();
//    index_info->GetIndex()->ScanKey()
//  }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
return false;
  if(ite == end){
    return false;
  }
  (*rid) = (*ite).second;
  row->SetRowId(*rid);
  table_info->GetTableHeap()->GetTuple(row, nullptr);
  ++ite;
  return true;
}
