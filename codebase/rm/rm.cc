
#include "rm.h"
#include <stdlib.h>




void RelationManage::createColumnRecordDescriptor(
        vector<Attribute> &recordDescriptor);


void RelationManager::prepareColumnRecord(vector<Attribute> &recordDescriptor);


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
}

RelationManager::~RelationManager(){
}

void RelationManager::prepareApiTableRecord(const int tableId, 
        const string &tableName, const string &fileName, void *data) {
   
    // data is memset to 0
    // null indicator only needs 1 byte, already set to 0
    int offset = 1;
    
    memcpy((char*)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)data + offset, tableName.c_str(), tableName.length());
    offset += tableName.length();

    memcpy((char*)data + offset, fileName.c_str() fileName.length()); 
}

void RelationManager::prepareApiColumnRecord(const int tableId, 
        const string &columnName, const AttrType type, 
        const int columnLength, const int position, void *data) {

    int offset = 1;         // null indicator is 1 byte, already set to 0
    
    memcpy((char*)data + offset, &tableId, sizeof(int));
    offset += sizeof(int);
    
    memcpy((char*)data + offset, columnName.c_str(), columnName.length());
    offset += columnName.length();

    memcpy((char*)data + offset, &type, sizeof(AttrType));
    offset += sizeof(AttrType);

    memcpy((char*)data + offset, &columnLength, sizeof(int));
    offset += sizeof(int);

    memcpy((char*)data + offset, &position, sizeof(int));
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
    // insert table records
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(1, TABLES_TABLE_NAME, TABLES_FILE_NAME, tmpData);
    if (rbfm->insetRecord(fileHandle, tableRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(2, COLUMNS_TABLE_NAME, COLUMNS_FILE_NAME, tmpData);
    if (rbfm->insetRecord(fileHandle, columnsRecordDescriptor, 
                tmpData, tableRid) < 0) {
        return -1;
    }
    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }
  

	//create and open column file
    //prepare column record -> insert
	if (rbfm->createFile(COLUMNS_FILE_NAME) < 0) {
        return -1;
    }
    if (rbfm->openFile(COLUMNS_FILE_NAME, fileHandle) < 0) {
        return -1;
    }
    
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "file-name", TypeVarChar, 50, 3, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }
    
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-type", TypeInt, 4, 3, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }
        
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-length", TypeInt, 4, 4, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-position", TypeInt, 4, 5, tmpData);
    if (rbfm->insetRecord(fileHandle, columnRecordDescriptor, 
                tmpData, dummyRid) < 0) {
        return -1;
    }

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }
    
    
    free(tmpData);
    return 0;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    RC rc = rbfm->openFile(tableName, fileHandle);

    if (rc == 0) {
    	getAttributes(tableName, recordDescriptor);

    	//is data returned contains only 1byte nullIndicator??
    	RC rc2 = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);

    	rbfm->closeFile(fileHandle);
    	return rc2;

    }else {
    	return -1;
    }
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
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

