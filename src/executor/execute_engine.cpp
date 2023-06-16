#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

    struct dirent *stdir;
    while((stdir = readdir(dir)) != nullptr) {
      if( strcmp( stdir->d_name , "." ) == 0 ||
          strcmp( stdir->d_name , "..") == 0 ||
          stdir->d_name[0] == '.')
        continue;
      dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      LOG(WARNING) << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      LOG(WARNING) << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      LOG(WARNING) << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      LOG(WARNING) << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      LOG(WARNING) << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      LOG(WARNING) << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      LOG(WARNING) << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      LOG(WARNING) << "Key not exists." << endl;
      break;
    case DB_QUIT:
      LOG(INFO) << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name;
  db_name = ast->child_->val_;
  if(dbs_.find(db_name) != dbs_.end()){
    return DB_ALREADY_EXIST;
  }
  auto new_db = new DBStorageEngine(db_name,true);
  dbs_.emplace(db_name,new_db);
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name;
  db_name = ast->child_->val_;
  if(dbs_.find(db_name) == dbs_.end()){
    return DB_NOT_EXIST;
  }
  remove(dbs_[db_name]->db_file_name_.c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  for(auto ite:dbs_){
    LOG(INFO)<<ite.first<<std::endl;
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name;
  db_name = ast->child_->val_;
  if(dbs_.find(db_name) == dbs_.end()){
    return DB_NOT_EXIST;
  }
  current_db_ = db_name;
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  vector<TableInfo *> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto ite = tables.begin();ite != tables.end();ite++){
    LOG(INFO)<<(*ite)->GetTableName()<<std::endl;
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  int flag;
  std::vector<Column *>columns;
  std::vector<std::string>primary_keys;
  pSyntaxNode temp1 = ast->child_;
  string table_name = temp1->val_;
  string column_name;
  string column_type;
  int column_count = 0;
  double char_double;
  int char_int;
  temp1 = temp1->next_->child_;
  pSyntaxNode temp2 = temp1;
  while(temp2->next_ != nullptr){
    temp2 = temp2->next_;
  }
  if(temp2->val_ != nullptr){
    if(strcmp(temp2->val_,"primary keys")==0){
      temp2 = temp2->child_;
      while(temp2 != nullptr){
        primary_keys.push_back(temp2->val_);
        temp2 = temp2->next_;
      }
    }
  }
  while(temp1 != nullptr) {
    if(temp1->val_ != nullptr) {
      if (strcmp(temp1->val_, "primary keys") == 0) {
        break;
      }
    }
    flag=0;
    temp2 = temp1;
    temp2 = temp1->child_;
    column_name = temp2->val_;
    temp2 = temp2->next_;
    column_type = temp2->val_;
    for(auto ite=primary_keys.begin();ite!=primary_keys.end();ite++){
      if((*ite) == column_name){
        flag=1;
        break;
      }
    }
    if(temp1->val_ != nullptr){
      if(strcmp(temp1->val_,"unique")==0){
        flag = 1;
      }
    }
    if (column_type == "int") {
      if(flag==1){
        columns.push_back(new Column(column_name, TypeId::kTypeInt, column_count, false, true));
      }
      else{
        columns.push_back(new Column(column_name, TypeId::kTypeInt, column_count, true, false));
      }
    } else if (column_type == "float") {
      if(flag==1){
        columns.push_back(new Column(column_name, TypeId::kTypeFloat, column_count, false, true));
      }
      else{
        columns.push_back(new Column(column_name, TypeId::kTypeFloat, column_count, true, false));
      }
    } else if (column_type == "char") {
      // 如果是正小数的话我在workbench试了下是可以的，应该自动转换成int了。-0.69这种不可以
      char_double = atof(temp2->child_->val_);
      char_int = char_double;
      if (char_double < 0) {
        LOG(WARNING)<<"syntax error "<<std::endl;
        return DB_FAILED;
      }
      if(flag==1){
        columns.push_back(new Column(column_name, TypeId::kTypeChar, char_int,column_count, false, true));
      }
      else{
        columns.push_back(new Column(column_name, TypeId::kTypeChar, char_int,column_count, true, false));
      }
    }
    temp1 = temp1->next_;
    column_count++;
  }
  Schema *schema = new Schema(columns);
  TableInfo *table_info;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateTable(table_name,schema,nullptr,table_info);
  if(result == DB_TABLE_ALREADY_EXIST){
    return DB_TABLE_ALREADY_EXIST;
  }
  string index_name = table_name+"_primary_key_index";//
  IndexInfo *index_info;
  dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,index_name,primary_keys, nullptr,index_info,"bptree");
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  string table_name = ast->child_->val_;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  return result;
}


dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  std::vector<TableInfo *> tables;
  string table_name;
  std::vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto ite=tables.begin();ite!=tables.end();ite++){
    table_name = (*ite)->GetTableName();
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name,indexes);
    for(auto ite_=indexes.begin();ite_!=indexes.end();ite_++){
      LOG(INFO)<<(*ite_)->GetIndexName()<<std::endl;
    }
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  pSyntaxNode temp = ast;
  temp = ast->child_;
  string index_name = temp->val_;
  temp=temp->next_;
  string table_name = temp->val_;
  temp=temp->next_;
  std::vector<std::string> index_keys;
  if(temp == nullptr){
    LOG(WARNING)<<"unknown mistake"<<std::endl;
    return DB_FAILED;
  }
  temp=temp->child_;
  while(temp != nullptr){
    index_keys.push_back(temp->val_);
    temp = temp->next_;
  }
  IndexInfo *index_info;
  dberr_t result = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name,index_name,index_keys, nullptr,index_info,"bptree");
  return result;
}


dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  string index_name = ast->child_->val_;
  string table_name;
  std::vector<TableInfo *> tables;
  std::vector<IndexInfo *> indexes;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto ite=tables.begin();ite!=tables.end();ite++){
    table_name = (*ite)->GetTableName();
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_name,indexes);
    for(auto ite_=indexes.begin();ite_!=indexes.end();ite_++){
      if((*ite_)->GetIndexName() == index_name){
        dbs_[current_db_]->catalog_mgr_->DropIndex(table_name,index_name);
        return DB_SUCCESS;
      }
    }
  }
  return DB_INDEX_NOT_FOUND;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  const int buf_size = 1024;
  char cmd[buf_size];
  string file_name = ast->child_->val_;
  ifstream file(file_name,ios::in);
  if(!file.is_open()){
    LOG(WARNING)<<"open file failed"<<std::endl;
    return DB_FAILED;
  }
  file>>noskipws;
  int count;
  char temp;
  while(!file.eof()){
    memset(cmd,0,buf_size);
    count=0;
    while(!file.eof()){
      file >> temp;
      cmd[count] = temp;
      count++;
      if(temp == ';'){
        break;
      }
    }
    //接收回车
    if(!file.eof()){
      file>>temp;
    }
    YY_BUFFER_STATE bp = yy_scan_string(cmd);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      return DB_FAILED;
    }
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if(MinisqlParserGetError()){
      printf("%s\n", MinisqlParserGetErrorMessage());
      return DB_FAILED;
    }
    auto result = this->Execute(MinisqlGetParserRootNode());
    this->ExecuteInformation(result);
  }
  return DB_SUCCESS;
}


dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
