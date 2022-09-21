#include <vector>
#include <string>
#include "cider/CiderBatch.h"
#include <fstream>
class CiderUtSqlGenerator {
  public:
    CiderUtSqlGenerator() {
        sql_outfile_.open("ut-sql.sql");

    }
    ~CiderUtSqlGenerator() {
        sql_outfile_.close();
    }
    std::vector<std::vector<std::string>> toTableVector(const CiderBatch& input_batch) const;
    std::string toInsertValuesSqlString(const CiderBatch& input_batch);

      // create a basic table and insert data for test, only int32 type support.
    void createTableAndInsertData(const std::string& table_name,
                                    const std::string& create_ddl,
                                    const std::vector<std::vector<int32_t>>& table_data);

    void createTableAndInsertData(const std::string& table_name,
                                    const std::string& create_ddl,
                                    const std::vector<std::shared_ptr<CiderBatch>>& data);

    void createTableAndInsertData(const std::string& table_name,
                                    const std::string& create_ddl,
                                    const std::shared_ptr<CiderBatch>& data);

    void outputQuerySql(const std::string& sql);
  private:
    std::ofstream sql_outfile_;
};