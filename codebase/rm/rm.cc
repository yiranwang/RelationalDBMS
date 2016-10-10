
#include "rm.h"
#include <stdlib.h>


void createTableRecordDescriptor(vector<Attribute> &recordDescriptor);

void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor);

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
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	//create table record descriptor -> prepare table record -> insert
	//create column record descriptor -> prepare column record -> insert
	FileHandle tableFileHandle;
	RID tableRid;

	void *tableData = malloc(PAGE_SIZE);
	unsigned char *nullIndicator = (unsigned char *)malloc(PAGE_SIZE);

	memset(nullIndicator, 0, sizeof(char));

	vector<Attribute> tableRecordDescriptor;

	//create "table-id", "table-name", "file-name" record desciptor in catalog table
	createTableRecordDescriptor(tableRecordDescriptor);

	rbfm->createFile("Tables");
	rbfm->openFile("Tables", tableFileHandle);

	//insert records of table "Tables", "Columns" to "Table"
	int tableRecordSize = 0;
	prepareTableRecord(nullIndicator, 1, "Tables", "Tables", tableData, tableRecordSize);
	rbfm->insertRecord(tableFileHandle, tableRecordDescriptor, tableData, tableRid);

	free(tableData);

	tableData = malloc(PAGE_SIZE);

	prepareTableRecord(nullIndicator, 2, "Columns", "Columns", tableData, tableRecordSize);
	rbfm->insertRecord(tableFileHandle, tableRecordDescriptor, tableData, tableRid);
	rbfm->closeFile(tableFileHandle);

	free(tableData);
	free(nullIndicator);

	//create "table-id", "table-name", "file-name"... record desciptor in catalog column
	void *columnData = malloc(PAGE_SIZE);
	FileHandle columnFileHandle;
	RID columnRid;
	int columnRecordSize = 0;

	unsigned char *nullIndicator2 = (unsigned char *)malloc(PAGE_SIZE);
	memset(nullIndicator2, 0, sizeof(char));

	vector<Attribute> columnRecordDescriptor;
	createColumnRecordDescriptor(columnRecordDescriptor);

	rbfm->createFile("Columns");
	rbfm->openFile("Columns", columnFileHandle);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 1, "table-id", TypeInt. 4, 1, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 1, "table-name", TypeVarChar. 50, 2, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 1, "file-name", TypeVarChar. 50, 3, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 2, "table-id", TypeInt. 4, 1, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 2, "column-name", TypeVarChar. 50, 2, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 2, "column-type", TypeInt. 4, 3, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 2, "column-length", TypeInt. 4, 4, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	prepareColumnRecord(nullIndicator2, nullIndicator2, 2, "column-position", TypeInt. 4, 5, columnData, columnRecordSize);
	rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, columnData, columnRid);

	rbfm->closeFile(columnFileHandle);

	free(nullIndicator2);
	free(columnData);

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
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
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

void createTableRecordDescriptor(vector<Attribute> &recordDescriptor) {
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

void createColumnRecordDescriptor(vector<Attribute> &recordDescriptor) {
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

	memcpy((char*)data + offset, tableName.c_str(), columnNameLength);
	offset += columnNameLength;

	*((AttrType*)((char*)data + offset)) = columnType;
	offset += sizeof(AttrType);

	*((int*)((char*)data + offset)) = columnLength;
	offset += sizeof(int);

	*((int*)((char*)data + offset)) = position;
	offset += sizeof(int);

	recordSize = offset;
}
