#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const {
    return (sizeof(uint32_t)*3 + table_meta_pages_.size()*(sizeof(table_id_t)+
     sizeof(page_id_t)) + index_meta_pages_.size()*(sizeof(index_id_t)+sizeof(page_id_t)));
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    if(init){
        catalog_meta_ = CatalogMeta::NewInstance();
        next_table_id_ = 0;
        next_index_id_ = 0;
    }
    else{
        Page * catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
        buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID,false);
        for(auto ite:catalog_meta_->table_meta_pages_){
          if(LoadTable(ite.first,ite.second) != DB_SUCCESS){
            LOG(WARNING)<<"table load failed"<<std::endl;
          }
        }
        for(auto ite:catalog_meta_->index_meta_pages_){
          if(LoadIndex(ite.first,ite.second) != DB_SUCCESS){
            LOG(WARNING)<<"index load failed"<<std::endl;
          }
        }
        next_table_id_ = catalog_meta_->GetNextTableId();
        next_index_id_ = catalog_meta_->GetNextIndexId();
    }
}

CatalogManager::~CatalogManager() {
     //After you finish the code for the CatalogManager section,
     //you can uncomment the commented code. Otherwise it will affect b+tree test
     FlushCatalogMetaPage();
     delete catalog_meta_;
     for (auto iter : tables_) {
       delete iter.second;
     }
     for (auto iter : indexes_) {
       delete iter.second;
     }
}


dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
    page_id_t table_mata_page_id;
    if(table_names_.find(table_name) != table_names_.end()){
        return DB_TABLE_ALREADY_EXIST;
    }
    TableHeap *new_table = TableHeap::Create(buffer_pool_manager_,schema,txn,log_manager_,lock_manager_);
    auto table_mata_page = buffer_pool_manager_->NewPage(table_mata_page_id);
    page_id_t root_page_id = new_table->GetFirstPageId();
    auto table_mata = TableMetadata::Create(next_table_id_,table_name,root_page_id,schema);
    table_mata->SerializeTo(table_mata_page->GetData());
    buffer_pool_manager_->UnpinPage(table_mata_page_id,true);

    catalog_meta_->table_meta_pages_.emplace(next_table_id_,table_mata_page_id);
    table_names_.emplace(table_name,next_table_id_);
    table_info = TableInfo::Create();
    table_info->Init(table_mata,new_table);
    tables_.emplace(next_table_id_,table_info);
    next_table_id_++;
    //catalog_mata_page和table的meta,都要实时更新到缓冲池中
    auto catalog_mata_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_mata_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
    return DB_SUCCESS;
    //对于表的主属性自动建立B+树索引
}


dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    auto table = table_names_.find(table_name);
    if(table == table_names_.end()){
        return DB_TABLE_NOT_EXIST;
    }
    table_info = tables_.find(table->second)->second;
    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    for(auto ite : tables_){
        tables.push_back(ite.second);
    }
    return DB_SUCCESS;
}


dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
    auto temp = table_names_.find(table_name);
    if(temp == table_names_.end()){
        return DB_TABLE_NOT_EXIST;
    }
    if(index_names_[table_name].find(index_name) != index_names_[table_name].end()){
        return DB_INDEX_ALREADY_EXIST;
    }
    table_id_t table_id = temp->second;
    auto columns = tables_[table_id]->GetSchema()->GetColumns();
    int flag;
    std::vector<uint32_t> key_map;
    for(auto ite1:index_keys){
        flag = 0;
        for(auto ite2:columns){
          if(ite1 == ite2->GetName()){
            flag = 1;
            key_map.push_back(ite2->GetTableInd());
            break;
          }
        }
        if(flag == 0){
          return DB_COLUMN_NAME_NOT_EXIST;
        }
    }
    page_id_t index_page_id;
    auto index_page = buffer_pool_manager_->NewPage(index_page_id);
    IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_,index_name,table_id,key_map);
    index_meta->SerializeTo(index_page->GetData());
    index_info = IndexInfo::Create();
    index_info->Init(index_meta,tables_[table_id],buffer_pool_manager_,index_type);
    buffer_pool_manager_->UnpinPage(index_page_id,true);

    catalog_meta_->index_meta_pages_.emplace(next_index_id_,index_page_id);
    auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);

    //index_names_.emplace();
    indexes_.emplace(next_index_id_,index_info);
    next_index_id_++;
    return DB_SUCCESS;
}


dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    auto table_name_index_name_index= index_names_.find(table_name);
    if(table_name_index_name_index == index_names_.end()){
        return DB_INDEX_NOT_FOUND;
    }
    auto index_name_index = table_name_index_name_index->second.find(index_name);
    if(index_name_index == table_name_index_name_index->second.end()){
        return DB_INDEX_NOT_FOUND;
    }
    auto index = index_name_index->second;
    index_info = indexes_.find(index)->second;
    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    auto table_name_index_name_index = index_names_.find(table_name);
    if(table_name_index_name_index == index_names_.end()){
        return DB_INDEX_NOT_FOUND;
    }
    for(auto ite : table_name_index_name_index->second){
        indexes.push_back(indexes_.find(ite.second)->second);
    }
    return DB_SUCCESS;
}


dberr_t CatalogManager::DropTable(const string &table_name) {
    if(table_names_.find(table_name) == table_names_.end()){
        return DB_TABLE_NOT_EXIST;
    }
    //delete index is also needed
    for(auto ite:index_names_[table_name]){
        DropIndex(table_name,ite.first);
    }
    table_id_t table_id = table_names_[table_name];
    table_names_.erase(table_name);
    tables_.erase(table_id);
    auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
    catalog_meta_->table_meta_pages_.erase(table_id);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
    return DB_SUCCESS;
}


dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    if(index_names_.find(table_name) == index_names_.end()){
        return DB_TABLE_NOT_EXIST;
    }
    auto index_map = index_names_[table_name];
    if(index_map.find(index_name) == index_map.end()){
        return DB_INDEX_NOT_FOUND;
    }
    index_id_t index_id = index_map[index_name];
    index_names_[table_name].erase(index_name);
    indexes_.erase(index_id);
    auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
    catalog_meta_->index_meta_pages_.erase(index_id);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
    return DB_SUCCESS;
}


dberr_t CatalogManager::FlushCatalogMetaPage() const {
    buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
    return DB_SUCCESS;
}


dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    if(tables_.find(table_id) != tables_.end()){
        LOG(WARNING)<<"unknown mistake"<<std::endl;
        return DB_FAILED;
    }
    auto page_to_load = buffer_pool_manager_->FetchPage(page_id);
    TableMetadata *table_meta;
    TableMetadata::DeserializeFrom(page_to_load->GetData(),table_meta);
    buffer_pool_manager_->UnpinPage(page_id,false);
    TableHeap * table_heap = TableHeap::Create(buffer_pool_manager_,table_meta->GetFirstPageId(),table_meta->GetSchema(),log_manager_,lock_manager_);
    TableInfo * table_info = TableInfo::Create();
    table_info->Init(table_meta,table_heap);
    table_names_.emplace(table_meta->GetTableName(),table_id);
    tables_.emplace(table_id,table_info);
    return DB_SUCCESS;
}


dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    if(indexes_.find(index_id) != indexes_.end()){
        LOG(WARNING)<<"unknown mistake"<<std::endl;
        return DB_FAILED;
    }
    auto page_to_load = buffer_pool_manager_->FetchPage(page_id);
    IndexMetadata *index_meta;
    IndexMetadata::DeserializeFrom(page_to_load->GetData(),index_meta);
    buffer_pool_manager_->UnpinPage(page_id,false);
    IndexInfo * index_info = IndexInfo::Create();
    index_info->Init(index_meta,tables_[index_meta->GetTableId()],buffer_pool_manager_);
    std::string table_name = tables_.find(index_meta->GetTableId())->second->GetTableName();
    auto temp = index_names_.find(table_name);
    if(temp == index_names_.end()){
        std::unordered_map<std::string, index_id_t> temp_map;
        temp_map.emplace(index_meta->GetIndexName(),index_id);
        index_names_.emplace(table_name,temp_map);
    }
    else{
        temp->second.emplace(index_meta->GetIndexName(),index_id);
    }
    indexes_.emplace(index_id,index_info);
    return DB_SUCCESS;
}


dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto table = tables_.find(table_id);
    if(table == tables_.end()){
        return DB_TABLE_NOT_EXIST;
    }
    table_info = table->second;
    return DB_SUCCESS;
}
