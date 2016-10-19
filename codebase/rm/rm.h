
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
const string TABLES_TABLE_NAME = "Tables";
const string TABLES_FILE_NAME = "Tables";
const string COLUMNS_TABLE_NAME = "Columns";
const string COLUMNS_FILE_NAME = "Columns";

typedef struct{
    Attribute attribute;
    int position;
} AttributeWithPosition;

//increasing comparator for getAttribute
class CompLess { 
public: 
    bool operator()(const AttributeWithPosition& attr1, const AttributeWithPosition attr2) { 
        return attr1.position < attr2.position; 
    }
};


// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() {};
    ~RM_ScanIterator() {};

    RBFM_ScanIterator rbfm_ScanIterator;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { 
        return rbfm_ScanIterator.getNextRecord(rid, data);
    };

    RC close() { 
        rbfm_ScanIterator.close();
        return 0; 
    };
};


// Relation Manager
class RelationManager
{
public:
    static RelationManager* instance();

    RecordBasedFileManager *rbfm;

    vector<Attribute> tableRecordDescriptor;

    vector<Attribute> columnRecordDescriptor;

    // the total number of tables created
    int tableCount;

   
    // ======= start of self defined functions =====
    void prepareApiTableRecord(const int tableId, const string &tableName, const string &fileName, 
            void *data, int &size); 
    void prepareApiColumnRecord(const int tableId, const string &columnName, const AttrType type, 
            const int columnLength, const int position, void *data);
    void getTableIdByTableName(int &tableId, RID &rid, const string &tableName);
    
    // ======= end of self defined functions =====
    
    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, 
            const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparison type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
    RC addAttribute(const string &tableName, const Attribute &attr);

    RC dropAttribute(const string &tableName, const string &attributeName);

protected:
    RelationManager();
    ~RelationManager();

private:
  static RelationManager *_rm;

  void createTableRecordDescriptor(vector<Attribute> &recordDescriptor);

  void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor);
};

#endif
