#include "CiderUtSqlGenerator.h"

#define PUSH_BY_TYPE(C_TYPE)                \
  {                                          \
    C_TYPE* buf = (C_TYPE*)table_ptr_[i];    \
    for (int j = 0; j < row_num_; j++) {     \
      if (buf[j] == std::numeric_limits<C_TYPE>::min() || std::to_string(buf[j]) == "inf") {   \
        row_data_.push_back("null");            \
      } else {                                \ 
        row_data_.push_back(std::to_string(buf[j]));            \
      }  \
    }                                        \
    break;                                   \
  }

std::vector<std::vector<std::string>> CiderUtSqlGenerator::toTableVector(const CiderBatch& input_batch) {
    std::vector<std::vector<std::string>> batch_data_;
    
    auto types = input_batch->schema()->getColumnTypes();
    for (auto i = 0; i < types.size(); i++) {
        std::vector<std::string> row_data_;
        // print by Type kind

        auto type = types[i];
        switch (type.kind_case()) {
            case ::substrait::Type::KindCase::kBool:{
            // PRINT_BY_TYPE(int8_t)
            int8_t* buf = (int8_t*)input_batch->column(i);    
            for (int j = 0; j < row_num_; j++) {  
                if (buf[j] == std::numeric_limits<int8_t>::min()) {
                row_data_.push_back("null");            
                } else {
                row_data_.push_back("cast(" + std::to_string(buf[j]) + " as BOOLEAN)"); 
                }
            }                                        
            break;
            }
            case ::substrait::Type::KindCase::kI8: {
            // PRINT_BY_TYPE(int8_t)
            int8_t* buf = (int8_t*)input_batch->column(i);    
            for (int j = 0; j < row_num_; j++) {  
                if (buf[j] == std::numeric_limits<int8_t>::min()) {
                row_data_.push_back("null");            
                } else {
                row_data_.push_back("cast(" + std::to_string(buf[j]) + " as tinyint)"); 
                }
            }                                        
            break;
            }
            case ::substrait::Type::KindCase::kI16: {
            // PRINT_BY_TYPE(int8_t)
            int16_t* buf = (int16_t*)input_batch->column(i);    
            for (int j = 0; j < row_num_; j++) {  
                if (buf[j] == std::numeric_limits<int16_t>::min()) {
                row_data_.push_back("null");            
                } else {
                row_data_.push_back("cast(" + std::to_string(buf[j]) + " as smallint)"); 
                }
            }                                        
            break;
            }
            case ::substrait::Type::KindCase::kI32:
            PUSH_BY_TYPE(int32_t)
            case ::substrait::Type::KindCase::kI64: 
            PUSH_BY_TYPE(int64_t)
            case ::substrait::Type::KindCase::kDate: {
            // PRINT_BY_TYPE(int8_t)
            int64_t* buf = (int64_t*)input_batch->column(i);    
            for (int j = 0; j < row_num_; j++) {  
                if (buf[j] == std::numeric_limits<int64_t>::min()) {
                row_data_.push_back("null");            
                } else {
                row_data_.push_back("from_unixtime(" + std::to_string(buf[j]) + ")"); 
                }
            }                                        
            break;
            }
            case ::substrait::Type::KindCase::kFp32:
            PUSH_BY_TYPE(float)
            case ::substrait::Type::KindCase::kFp64:
            case ::substrait::Type::KindCase::kDecimal:
            PUSH_BY_TYPE(double)
            case ::substrait::Type::KindCase::kFixedChar:
            case ::substrait::Type::KindCase::kVarchar: 
            case ::substrait::Type::KindCase::kString: {
            // ss << "column type: CiderByteArray ";
            CiderByteArray* buf = (CiderByteArray*)input_batch->column(i);
            for (int j = 0; j < row_num_; j++) {
                row_data_.push_back("'" + CiderByteArray::toString(buf[j]) + "'");
            }
            break;
            }
            default:
            throw std::runtime_error("Not supported type to print value!");
        }
        batch_data_.emplace_back(row_data_);
    }
    std::vector<std::vector<std::string>> table_data_;
    for (int i = 0; i < row_num_; i++) {
        std::vector<std::string> table_row_;
        for (int j = 0; j < types.size(); j++) {
        table_row_.push_back(batch_data_[j][i]);
        }
        table_data_.emplace_back(table_row_);
    }

    return table_data_;

};
// std::string toInsertValuesSqlString(const CiderBatch& input_batch);

// // create a basic table and insert data for test, only int32 type support.
// void createTableAndInsertData(const std::string& table_name,
//                             const std::string& create_ddl,
//                             const std::vector<std::vector<int32_t>>& table_data);

// void createTableAndInsertData(const std::string& table_name,
//                             const std::string& create_ddl,
//                             const std::vector<std::shared_ptr<CiderBatch>>& data);

// void createTableAndInsertData(const std::string& table_name,
//                             const std::string& create_ddl,
//                             const std::shared_ptr<CiderBatch>& data);

// void outputQuerySql(const std::string& sql);