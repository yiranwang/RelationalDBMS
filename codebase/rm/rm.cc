
#include "rm.h"
#include <stdlib.h>
#include <fstream>

bool fexists(const std::string& filename);

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance(){
    if(!_rm)
        _rm = new RelationManager();
    return _rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();

	createTableRecordDescriptor(tableRecordDescriptor);

	createColumnRecordDescriptor(columnRecordDescriptor);

    countTableNumber = 0;
}


RelationManager::~RelationManager(){
}


void RelationManager::prepareApiTableRecord(const int tableId, const string &tableName, 
        const string &fileName, void *data, int &size) {
   
    // data is memset to 0
    // null indicator only needs 1 byte, already set to 0
    int offset = 1;
    
    // write tableId
    memcpy((char*)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);

    // write tableName length
    *(int*)((char*)data + offset) = tableName.length();
    offset += sizeof(int);
    // write tableName VarChar
    memcpy((char*)data + offset, tableName.c_str(), tableName.length());
    offset += tableName.length();

<<<<<<< Updated upstream
    // write fileName length
    *(int*)((char*)data + offset) = fileName.length();
    offset += sizeof(int);
    // write fileName VarChar
    memcpy((char*)data + offset, fileName.c_str(), fileName.length());
    offset += fileName.length();
    size = offset; 
=======
    memcpy((char*)data + offset, fileName.c_str(), fileName.length());

    if (DEBUG) {
        fprintf(stderr, "tableName is : %s\n", tableName.c_str());
    }
>>>>>>> Stashed changes
}


void RelationManager::prepareApiColumnRecord(const int tableId, const string &columnName, 
        const AttrType type, const int columnLength, const int position, void *data) {

    int offset = 1;         // null indicator is 1 byte, already set to 0
    
    // write tableId
    memcpy((char*)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);

    // write columnName length
    *(int*)((char*)data + offset) = columnName.length();
    offset += sizeof(int);
    // write columnName varChar
    *(int*)((char*)data + offset) = columnName.length();
    offset += columnName.length();

    // write AttrType
    *(AttrType*)((char*)data + offset) = type;
    offset += sizeof(AttrType);

    *(int*)((char*)data + offset) = columnLength;
    offset += sizeof(int);

<<<<<<< Updated upstream
    *(int*)((char*)data + offset) = position;
    offset += sizeof(int);
=======
    memcpy((char*)data + offset, &position, sizeof(int));

    if (DEBUG) {
        fprintf(stderr, "columnName is : %s\n", columnName.c_str());
    }
>>>>>>> Stashed changes
}


RC RelationManager::createCatalog(){
	//create column record descriptor -> prepare column record -> insert
	FileHandle fileHandle;

	//create and open table file
    //prepare table record -> insert
	if (rbfm->createFile(TABLES_FILE_NAME) < 0) {
        return -1;
    }
    if (rbfm->openFile(TABLES_FILE_NAME, fileHandle) < 0) {
        return -1;
    }
	void *tmpData = malloc(PAGE_SIZE);
    RID dummyRid;
    memset(tmpData, 0, PAGE_SIZE);
<<<<<<< Updated upstream
    int apiTableRecordSize = 0;


    prepareApiTableRecord(1, TABLES_TABLE_NAME, TABLES_FILE_NAME, tmpData, apiTableRecordSize);

    if (rbfm->insertRecord(fileHandle, tableRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(2, COLUMNS_TABLE_NAME, COLUMNS_FILE_NAME, tmpData, apiTableRecordSize);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
=======
    prepareApiTableRecord(1, TABLES_TABLE_NAME, TABLES_FILE_NAME, tmpData);

    if (DEBUG) {
        printf("prepareApiTableRecord done.\n");
    }

    if (rbfm->insertRecord(fileHandle, tableRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }
    if (DEBUG) {
        printf("insertRecord done.\n");
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(2, COLUMNS_TABLE_NAME, COLUMNS_FILE_NAME, tmpData);

    if (DEBUG) {
        fprintf(stderr, "prepareApiTableRecord done.\n");
    }

    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
>>>>>>> Stashed changes
        return -1;
    }

    if (DEBUG) {
        fprintf(stderr, "insertRecord done.\n");
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }
  
	//create and open column file
    //prepare and insert column records
	if (rbfm->createFile(COLUMNS_FILE_NAME) < 0) {
        return -1;
    }
    if (rbfm->openFile(COLUMNS_FILE_NAME, fileHandle) < 0) {
        return -1;
    }
    
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "file-name", TypeVarChar, 50, 3, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
    
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-type", TypeInt, 4, 3, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
        
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-length", TypeInt, 4, 4, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-position", TypeInt, 4, 5, tmpData);
    if (rbfm->insertRecord(fileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }
    
    
    free(tmpData);

    countTableNumber = 2;

    return 0;
}

RC RelationManager::deleteCatalog()
{
    if(rbfm->destroyFile(TABLES_FILE_NAME) < 0) {
        return -1;
    }       
    if(rbfm->destroyFile(COLUMNS_FILE_NAME) < 0) {
        return -1;
    }
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    countTableNumber++;

    RID rid;
    void *data = malloc(PAGE_SIZE);
    FileHandle fileHandle;

    string fileName = tableName;
    if (rbfm->createFile(fileName) < 0) {
        return -1;
    }

    int apiTableRecordSize = 0;
    prepareApiTableRecord(countTableNumber, tableName, fileName, data, apiTableRecordSize);
    printf("created apiTableRecord with size: %d\n", apiTableRecordSize);

    if (insertTuple("Tables", data, rid) < 0) {
        return -1;
    }

    Attribute attribute;

    for (int i = 0; i < attrs.size(); i++) {
        printf("iteration:%d\n", i);
        attribute = attrs[i];
        prepareApiColumnRecord(countTableNumber, attribute.name, attribute.type, attribute.length, i + 1, data);
        if (insertTuple("Columns", data, rid) < 0) {
            return -1;
        }
    }

    free(data);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{   
    if (tableName == "Tables" || tableName == "Columns" || !fexists(tableName)) {
        return -1;
    }

    if (rbfm->destroyFile(tableName) < 0) {
        return -1;
    }

    int tableId;
    RID rid;
    
    getTableIdByTableName(tableId, rid, tableName);

    if (deleteTuple("Tables", rid) < 0) {
        return -1;
    }

    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    RM_ScanIterator rm_ScanIterator;
    void *tupleDeleted = malloc(PAGE_SIZE);

    //search in "Tables" to find the corresponding tuples according to table-id
    scan("Columns", "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
    while (rm_ScanIterator.getNextTuple(rid, tupleDeleted) != -1) {
        if (deleteTuple("Columns", rid) < 0) {
            return -1;
        }
    }
    rm_ScanIterator.close();
    free(tupleDeleted);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    if (DEBUG) {
        printf("pussy");
    }
    int tableId;
    RID rid;
    if (DEBUG) {
        fprintf(stderr, "tableName is :%s\n", tableName.c_str());
    }
    
    getTableIdByTableName(tableId, rid, tableName);

    if (DEBUG) {
        fprintf(stderr, "tableName is :%s\n", tableName.c_str());
        //fprintf(stderr, "tableId is :%d\n", tableId);
    }

    vector<string> attributeNames;
    attributeNames.push_back("column-name");
    attributeNames.push_back("column-type");
    attributeNames.push_back("column-length");

    RBFM_ScanIterator rbfm_ScanIterator;
    FileHandle fileHandle;

    if (rbfm->openFile("Columns", fileHandle) < 0) {
        return -1;
    }
    rbfm->scan(fileHandle, columnRecordDescriptor, "table-id", EQ_OP, &tableId, attributeNames, rbfm_ScanIterator);

    void *recordData = malloc(PAGE_SIZE);

    Attribute attri;
    int attriLength;
    char* attriName = (char*)malloc(51);
    int attriNameLength;
    AttrType attriType;
    int offset = 0;
    
    while (rbfm_ScanIterator.getNextRecord(rid, recordData) != -1) {
        offset = 1;

        memcpy(&attriNameLength, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(attriName, (char*)recordData + offset, attriNameLength);
        offset += attriNameLength;

        memcpy(&attriType, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(&attriLength, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        attriName[attriNameLength] = '\0';
        attri.name = attriName;
        attri.type = attriType;
        attri.length = attriLength;
        attrs.push_back(attri);
    }

    rbfm_ScanIterator.close();
    free(recordData);
    free(attriName);
    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{   
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, 
        const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->updateRecord(fileHandle, recordDescriptor, data, rid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, 
        void *data) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, 
        const void *data) {
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, 
        const string &attributeName, void *data) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    RBFM_ScanIterator rbfm_ScanIterator;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->scan(fileHandle, recordDescriptor , conditionAttribute, compOp, value, attributeNames, 
        rbfm_ScanIterator) < 0) {
        return -1;
    }
    rm_ScanIterator.rbfm_ScanIterator = rbfm_ScanIterator;

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

void RelationManager::createTableRecordDescriptor(vector<Attribute> &recordDescriptor) {
	Attribute attr1;
	attr1.name = "table-id";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	recordDescriptor.push_back(attr1);

	Attribute attr2;
	attr2.name = "table-name";
	attr2.type = TypeVarChar;
	attr2.length = (AttrLength)50;
	recordDescriptor.push_back(attr2);

	Attribute attr3;
	attr3.name = "file-name";
	attr3.type = TypeVarChar;
	attr3.length = (AttrLength)50;
	recordDescriptor.push_back(attr3);
}

void RelationManager::createColumnRecordDescriptor(vector<Attribute> &recordDescriptor) {
	Attribute attr1;
	attr1.name = "table-id";
	attr1.type = TypeInt;
	attr1.length = (AttrLength)4;
	recordDescriptor.push_back(attr1);

	Attribute attr2;
	attr2.name = "column-name";
	attr2.type = TypeVarChar;
	attr2.length = (AttrLength)50;
	recordDescriptor.push_back(attr2);

	Attribute attr3;
	attr3.name = "column-type";
	attr3.type = TypeInt;
	attr3.length = (AttrLength)4;
	recordDescriptor.push_back(attr3);

	Attribute attr4;
	attr4.name = "column-length";
	attr4.type = TypeInt;
	attr4.length = (AttrLength)4;
	recordDescriptor.push_back(attr4);

	Attribute attr5;
	attr5.name = "column-position";
	attr5.type = TypeInt;
	attr5.length = (AttrLength)4;
	recordDescriptor.push_back(attr5);
}

void RelationManager::getTableIdByTableName(int &tableId, RID &rid, const string &tableName) {
    vector<string> attributeNames;
    attributeNames.push_back("table-id");

    //search in "Tables" to find the corresponding table-id for tableName
    FileHandle fileHandle;
    RBFM_ScanIterator rbfm_ScanIterator;
    rbfm->openFile("Tables", fileHandle);
    rbfm->scan(fileHandle, tableRecordDescriptor, "table-name", EQ_OP, tableName.c_str(), attributeNames, rbfm_ScanIterator);

    void *tupleDeleted = malloc(PAGE_SIZE);

    rbfm_ScanIterator.getNextRecord(rid, tupleDeleted);

    tableId = *(int*)((char*)tupleDeleted + 1);

    rbfm_ScanIterator.close();
    free(tupleDeleted);
    rbfm->closeFile(fileHandle);
}

bool fexists(const std::string& filename) {
  std::ifstream ifile(filename.c_str());
  return (bool)ifile;
}

