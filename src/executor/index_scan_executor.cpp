#include "executor/executors/index_scan_executor.h"
#include <algorithm>
#include "planner/expressions/constant_value_expression.h"

/**
 * TODO: Student Implement
 */
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {

}
bool CompareRowId(RowId &a, RowId &b){
  if(a.GetPageId() < b.GetPageId() || (a.GetPageId() == b.GetPageId() && a.GetSlotNum() < b.GetSlotNum())) return true;
  else return false;
}
void IndexScanExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),info);
  FindPredicates(plan_->GetPredicate());
  while(!expressions.empty()){
    //获取当前要检查的表达式
    auto operator_value = reinterpret_cast<ComparisonExpression *>(expressions.top().get())->GetComparisonType();
    auto column =  expressions.top().get()->GetChildAt(0);
    auto const_num =  expressions.top().get()->GetChildAt(1);
    uint32_t col_id =  dynamic_cast<ColumnValueExpression *>(column.get())->GetColIdx();
    //查找对应的索引
    for(auto index: plan_->indexes_){
      vector<RowId> new_result;
      if(index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_id){
        auto &num_value = reinterpret_cast<ConstantValueExpression *>(const_num.get())->val_;
        Row *key = new Row(*new vector<Field>(1, num_value));
        index->GetIndex()->ScanKey(*key, new_result, nullptr, operator_value);
        int i;
        if(result.empty()){
          result.assign(new_result.begin(), new_result.end());
        }
        else{
          sort(new_result.begin(), new_result.end(),CompareRowId);
          sort(result.begin(), result.end(),CompareRowId);
          result.resize(min(result.size(), new_result.size()));
          auto it = set_intersection(result.begin(),result.end(),new_result.begin(),new_result.end(), result.begin(), CompareRowId);
          result.resize(it - result.begin());
        }
      }
    }
    expressions.pop();
  }
  result_i =0;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  Row *new_row = nullptr;
  int flag;
  while(result_i < result.size()) {
    flag = 0;
    RowId row_id = result[result_i];
    new_row = new Row(row_id);
    if (!info->GetTableHeap()->GetTuple(new_row, nullptr)) {
      return false;
    }
    result_i++;
    if (plan_->need_filter_) {
      if (plan_->GetPredicate()->Evaluate(new_row).CompareEquals(Field(kTypeInt, 1))) {
        flag=1;
        break;
      }
    } else{flag=1;
      break;}
  }
  if(flag == 1){}
  else return false;

  vector<Field> fields;
  for (auto output_column : plan_->OutputSchema()->GetColumns()) {
    for (auto column : info->GetSchema()->GetColumns()) {
      if (output_column->GetName() == column->GetName()) fields.push_back(*new_row->GetField(column->GetTableInd()));
    }
  }
  *row = Row(fields);
  *rid = RowId(new_row->GetRowId());
  delete new_row;
  return true;
}
void IndexScanExecutor::FindPredicates(AbstractExpressionRef predicate){
  if(predicate->GetType()==  ExpressionType::ComparisonExpression){
    expressions.push(predicate);
    return ;
  }
  else if(predicate->GetType() ==  ExpressionType::LogicExpression){
    auto children= predicate->GetChildren();
    for(auto child :children){
      FindPredicates(child);
    }
    return ;
  }
  else return;
}