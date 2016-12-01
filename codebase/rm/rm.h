
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

static const char TABLES_TABLE_NAME[]  = "Tables";
static const char TABLES_FILE_NAME[]   = "Tables";
static const char COLUMNS_TABLE_NAME[] = "Columns";
static const char COLUMNS_FILE_NAME[]  = "Columns";
static const char INDICES_TABLE_NAME[] = "Indices";
static const char INDICES_FILE_NAME[]  = "Indices";

typedef struct{
    Attribute attribute;
    int position;
} AttributeWithPosition;

//increasing comparator for sort attributes in the function getAttribute
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
        RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        rbfm->closeFile(rbfm_ScanIterator.fileHandle);
        rbfm_ScanIterator.close();
        return 0; 
    };
};


// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator() {};  	// Constructor
    ~RM_IndexScanIterator() {}; 	// Destructor

    IX_ScanIterator ix_ScanIterator;

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) {
        return ix_ScanIterator.getNextEntry(rid, key);
    };  	// Get next matching entry
    RC close() {
        return ix_ScanIterator.close();
    };             			// Terminate index scan
};


// Relation Manager
class RelationManager
{
public:
    static RelationManager* instance();

    RecordBasedFileManager *rbfm;

    IndexManager *ixm;

    vector<Attribute> tableRecordDescriptor;

    vector<Attribute> columnRecordDescriptor;

    vector<Attribute> indicesRecordDescriptor;

    // the latest table id (or largest, since it's increasing monotonically)
    int lastTableId;

    bool creatingTable;
    bool deletingTable;
    bool insertingIndex;
    bool deletingIndex;

   
    // ======= start of self defined functions =====
    void prepareApiTableRecord(const int tableId, const string &tableName, const string &fileName, void *data, int &size); 
    void prepareApiColumnRecord(const int tableId, const string &columnName, const AttrType type, const int columnLength, const int position, void *data);
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



    RC createIndex(const string &tableName, const string &attributeName);

    RC destroyIndex(const string &tableName, const string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const string &tableName,
                 const string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);

    void insertIndexWithInsertTuple(FileHandle &fileHandle, const string &tableName,
                                  vector<Attribute> &recordDescriptor, const RID &rid);

    void deleteIndexWithDeleteTuple(FileHandle &fileHandle, const string &tableName,
                                  vector<Attribute> &recordDescriptor, const RID &rid);

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

  void createIndicesRecordDescriptor(vector<Attribute> &recordDescriptor);
};

#endif
