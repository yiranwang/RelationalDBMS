
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

    tableCount = 0;
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

    // write fileName length
    *(int*)((char*)data + offset) = fileName.length();
    offset += sizeof(int);
    // write fileName VarChar
    memcpy((char*)data + offset, fileName.c_str(), fileName.length());
    offset += fileName.length();
    size = offset; 

}


void RelationManager::prepareApiColumnRecord(const int tableId, const string &columnName, 
        const AttrType type, const int columnLength, const int position, void *data) {

    int offset = 1;         // null indicator is 1 byte, already set to 0
    
    // write tableId
    *(int*)((char*)data + offset) = tableId;
    offset += sizeof(int);

    // write columnName length
    *(int*)((char*)data + offset) = columnName.length();
    offset += sizeof(int);
    // write columnName varChar
    memcpy((char*)data + offset, columnName.c_str(), columnName.length());
    offset += columnName.length();

    // write AttrType
    *(AttrType*)((char*)data + offset) = type;
    offset += sizeof(AttrType);

    *(int*)((char*)data + offset) = columnLength;
    offset += sizeof(int);

    *(int*)((char*)data + offset) = position;
    offset += sizeof(int);

}


RC RelationManager::createCatalog(){
	//create column record descriptor -> prepare column record -> insert
	FileHandle tableFileHandle;

	//create and open table file
    //prepare table record -> insert
	if (rbfm->createFile(TABLES_FILE_NAME) < 0) {
        return -1;
    }
    if (rbfm->openFile(TABLES_FILE_NAME, tableFileHandle) < 0) {
        return -1;
    }
	void *tmpData = malloc(PAGE_SIZE);
    RID dummyRid;
    memset(tmpData, 0, PAGE_SIZE);

    int apiTableRecordSize = 0;

    prepareApiTableRecord(1, TABLES_TABLE_NAME, TABLES_FILE_NAME, tmpData, apiTableRecordSize);
    if (rbfm->insertRecord(tableFileHandle, tableRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    //printf("Inserted below into Tables:\n");
    //rbfm->printRecord(tableRecordDescriptor, tmpData);

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(2, COLUMNS_TABLE_NAME, COLUMNS_FILE_NAME, tmpData, apiTableRecordSize);
    if (rbfm->insertRecord(tableFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    //printf("Inserted below into Tables:\n");
    //rbfm->printRecord(tableRecordDescriptor, tmpData);

    if (rbfm->closeFile(tableFileHandle) < 0) {
        return -1;
    }





      
	//create and open column file
    //prepare and insert column records
    FileHandle columnFileHandle;
	if (rbfm->createFile(COLUMNS_FILE_NAME) < 0) {
        return -1;
    }
    if (rbfm->openFile(COLUMNS_FILE_NAME, columnFileHandle) < 0) {
        return -1;
    }
    

    // ============== Tables fields

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
    printf("table-id is inserted to RID(%d, %d)\n", dummyRid.pageNum, dummyRid.slotNum);

    //printf("Inserted below into Columns:\n");
    //rbfm->printRecord(columnRecordDescriptor, tmpData);

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
    printf("table-name is inserted to RID(%d, %d)\n", dummyRid.pageNum, dummyRid.slotNum);


    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "file-name", TypeVarChar, 50, 3, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
    printf("file-name is inserted to RID(%d, %d)\n", dummyRid.pageNum, dummyRid.slotNum);



    // ============== Columns fields

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
    printf("table-id is inserted to RID(%d, %d)\n", dummyRid.pageNum, dummyRid.slotNum);

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-type", TypeInt, 4, 3, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }
        
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-length", TypeInt, 4, 4, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-position", TypeInt, 4, 5, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(columnFileHandle) < 0) {
        return -1;
    }
    
    free(tmpData);
    tableCount = 2;

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

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    
    tableCount++;
    RID rid;

    // create a file for this table to store pages of records
    string fileName = tableName;
    if (rbfm->createFile(fileName) < 0) {
        return -1;
    }

    // prepare and insert table info into Tables
    int apiTableRecordSize = 0;

    void *data = malloc(PAGE_SIZE);
    prepareApiTableRecord(tableCount, tableName, fileName, data, apiTableRecordSize);

    //printf("created apiTableRecord to be inserted to Tables: \n");
    //printTuple(tableRecordDescriptor, data); // (3, tbl_employee, tbl_employee)

    if (insertTuple(TABLES_TABLE_NAME, data, rid) < 0) {                                    // ######### BUG
        return -1;
    }

    // prepare and insert record descriptor info into Columns
    Attribute attribute;
    for (int i = 0; i < attrs.size(); i++) {
        attribute = attrs[i];
        memset(data, 0, PAGE_SIZE);
        prepareApiColumnRecord(tableCount, attribute.name, attribute.type, attribute.length, i + 1, data);

        if (insertTuple("Columns", data, rid) < 0) {
            return -1;
        }
    }

    free(data);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName) {   
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


// read recordDescriptor out from Columns
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {

    int tableId = 0;
    RID rid;
    
    getTableIdByTableName(tableId, rid, tableName);

    vector<string> attributeNames;
    attributeNames.push_back("column-name");
    attributeNames.push_back("column-type");
    attributeNames.push_back("column-length");
    
    // TODO: construct attribute with position
    // attributeNames.push_back("column-position");
    

    FileHandle fileHandle;

    if (rbfm->openFile(COLUMNS_FILE_NAME, fileHandle) < 0) {
        printf("Error in openFile%s\n", COLUMNS_FILE_NAME.c_str());
        return -1;
    }

    //printf("In getAttributes, before scan Columns, file %s opened. fileHandle.fd = %d\n", COLUMNS_FILE_NAME.c_str(), fileHandle.fd);


    RBFM_ScanIterator rbfm_ScanIterator;
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


    // TODO: sort the recordDescriptor by position


    printf("getAttributes done\n");
    return 0;
}



RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {   

    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);                         

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }


    //printf("inside insertTuple. \n");
    //printf("tableRD\treturned\n");
    //for (int i = 0; i < recordDescriptor.size(); i++) {
    //    printf("%s\t%s\n", tableRecordDescriptor[i].name.c_str(), recordDescriptor[i].name.c_str());
    //}
    
    
    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
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

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
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

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    void *targetInnerRecord = malloc(PAGE_SIZE);
    if(rbfm->readInnerRecord(fileHandle, recordDescriptor, rid, targetInnerRecord) < 0) {
        printf("Error in readInnerRecord\n");
        return -1;
    }

    // find out condition attribute index
    int conditionAttrIndex = 0;
    for (; conditionAttrIndex < recordDescriptor.size(); conditionAttrIndex++) {
        if (recordDescriptor[conditionAttrIndex].name.compare(attributeName) == 0) {
            break;
        }
    }
    if (conditionAttrIndex == recordDescriptor.size()) {
        printf("conditionAttribute not found!\n");
        return -1;
    }


    // read out the attribute from the target record    
    if (rbfm->readAttributeFromInnerRecord(recordDescriptor, targetInnerRecord, conditionAttrIndex, data) < 0) {
        printf("Error in readAttributeFromRecord\n");
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



// find tableId of tableName in Tables
void RelationManager::getTableIdByTableName(int &tableId, RID &rid, const string &tableName) {

    vector<string> attributeNames;
    attributeNames.push_back("table-id");

    //search in "Tables" to find the corresponding table-id of tableName
    FileHandle fileHandle;
    RBFM_ScanIterator rbfm_ScanIterator;
    rbfm->openFile("Tables", fileHandle);
    rbfm->scan(fileHandle, tableRecordDescriptor, "table-name", EQ_OP, tableName.c_str(), 
        attributeNames, rbfm_ScanIterator);

    void *innerRecord = malloc(PAGE_SIZE);

    rbfm_ScanIterator.getNextRecord(rid, innerRecord);
    //printf("In getTableIdByName. The record containing desired table-id is:\n");
    //rbfm->printRecord(rbfm_ScanIterator.projectedDescriptor, innerRecord);


    tableId = *(int*)((char*)innerRecord + 1);
    rbfm_ScanIterator.close();
    free(innerRecord);
    rbfm->closeFile(fileHandle);

    //printf("table %s has a tableId of: %d\n", tableName.c_str(), tableId);            // ##### CORRECT !!!
}


bool fexists(const std::string& filename) {
  std::ifstream ifile(filename.c_str());
  return (bool)ifile;
}

