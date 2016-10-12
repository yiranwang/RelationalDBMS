
#include "rm.h"
#include <stdlib.h>

void prepareTableRecord(unsigned char *nullIndicator, const int tableId, const string &tableName, const string &fileName, void *data, int &recordSize);

void prepareColumnRecord(unsigned char *nullIndicator, const int tableId, const string &columnName, const AttrType columnType , const int columnLength, const int position, void *data, int &recordSize);


RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
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

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{

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

void prepareTableRecord(unsigned char *nullIndicator, const int tableId, const string &tableName, const string &fileName, void *data, int &recordSize) {
	int tableNameLength = tableName.size();
	int fileNameLength = fileName.size();

	int offset = 0;

	memcpy(data, nullIndicator, sizeof(char));
	offset += sizeof(char);

	*((int*)((char*)data + offset)) = tableId;
	offset += sizeof(int);

	*((int*)((char*)data + offset)) = tableNameLength;
    offset += sizeof(int);
    
    memcpy((char*)data + offset, tableName.c_str(), tableNameLength);
    offset += tableNameLength;
    
    *((int*)((char*)data + offset)) = fileNameLength;
    offset += sizeof(int);
    
    memcpy((char*)data + offset, fileName.c_str(), fileNameLength);
    offset += fileNameLength;

	recordSize = offset;
}

void prepareColumnRecord(unsigned char *nullIndicator, const int tableId, const string &columnName, const AttrType columnType , const int columnLength, const int position, void *data, int &recordSize) {
	int columnNameLength = columnName.size();

	int offset = 0;

	memcpy(data, nullIndicator, sizeof(char));
	offset += sizeof(char);

	*((int*)((char*)data + offset)) = tableId;
	offset += sizeof(int);

	memcpy((char*)data + offset, columnName.c_str(), columnNameLength);
	offset += columnNameLength;

	*((AttrType*)((char*)data + offset)) = columnType;
	offset += sizeof(AttrType);

	*((int*)((char*)data + offset)) = columnLength;
	offset += sizeof(int);

	*((int*)((char*)data + offset)) = position;
	offset += sizeof(int);

	recordSize = offset;
}
