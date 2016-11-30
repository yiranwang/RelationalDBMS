
#include "rm.h"

#include <stdio.h>
#include <string.h>

#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <assert.h>


bool fexists(const string& fileName) {
    //printf("Checking if file %s exists...\n", fileName.c_str());
    return access(fileName.c_str(), F_OK ) != -1;
}

RM_IndexScanIterator::RM_IndexScanIterator(){

}

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance(){
    if(!_rm)
        _rm = new RelationManager();
    return _rm;
}

RelationManager::RelationManager() {
	rbfm = RecordBasedFileManager::instance();

    ixm = IndexManager::instance();

	createTableRecordDescriptor(tableRecordDescriptor);

	createColumnRecordDescriptor(columnRecordDescriptor);


    // if the catalog exists, scan the Tables to find the lastTableId
    if (fexists("Tables")) {

        //printf("%s exists, scanning Tables...\n", TABLES_FILE_NAME.c_str());

        FileHandle fh;
        rbfm->openFile(TABLES_FILE_NAME, fh);

        RBFM_ScanIterator rbsi;
        string attr = "table-id";
        int idVal = 1;
        vector<string> attributes;      // projected
        attributes.push_back(attr);
        rbfm->scan(fh, tableRecordDescriptor, attr, NO_OP, &idVal, attributes, rbsi);


        RID rid = {};
        void *returnedData = malloc(PAGE_SIZE);

        while (rbsi.getNextRecord(rid, returnedData) != RBFM_EOF) {
            int curTableID = *(int*)((char*)returnedData + 1);
            if (lastTableId < curTableID) {
                lastTableId = curTableID;
            }
        }

        free(returnedData);
        rbsi.close();
        rbfm->closeFile(fh);

    } else {
        //printf("%s doesn't exist, set lastTableId = 0\n", TABLES_FILE_NAME.c_str());
        lastTableId = 0;
    }

}


RelationManager::~RelationManager(){
}


void RelationManager::prepareApiTableRecord(const int tableId, const string &tableName, const string &fileName, void *data, int &size) {
   
    // null indicator only needs 1 byte, already set to 0
    memset(data, 0, PAGE_SIZE);
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


void RelationManager::prepareApiColumnRecord(const int tableId, const string &columnName, const AttrType type, const int columnLength, const int position, void *data) {

    memset(data, 0, PAGE_SIZE);
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


RC RelationManager::createCatalog() {
	//create column record descriptor -> prepare column record -> insert
	FileHandle tableFileHandle;

    // ==============   begin Tables ================ 
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
        free(tmpData);
        rbfm->closeFile(tableFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(2, COLUMNS_TABLE_NAME, COLUMNS_FILE_NAME, tmpData, apiTableRecordSize);
    if (rbfm->insertRecord(tableFileHandle, tableRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(tableFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiTableRecord(3, INDICES_TABLE_NAME, INDICES_FILE_NAME, tmpData, apiTableRecordSize);
    if (rbfm->insertRecord(tableFileHandle, tableRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(tableFileHandle);
        return -1;
    }

    if (rbfm->closeFile(tableFileHandle) < 0) {
        free(tmpData);
        return -1;
    }


    // ==============   finished Tables ================

    // ==============   begin Columns ================  

	//create and open column file
    //prepare and insert column records
    FileHandle columnFileHandle;
	if (rbfm->createFile(COLUMNS_FILE_NAME) < 0) {
        free(tmpData);
        return -1;
    }
    if (rbfm->openFile(COLUMNS_FILE_NAME, columnFileHandle) < 0) {
        free(tmpData);
        return -1;
    }
    

        // ============== write Tables record descriptor

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "table-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(1, "file-name", TypeVarChar, 50, 3, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

        // ============== write Columns record descriptor

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-type", TypeInt, 4, 3, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }
        
    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-length", TypeInt, 4, 4, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(2, "column-position", TypeInt, 4, 5, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }


    // ============== write Indices record descriptor


    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(3, "table-id", TypeInt, 4, 1, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(3, "index-name", TypeVarChar, 50, 2, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(3, "index-type", TypeInt, 4, 3, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(3, "index-length", TypeInt, 4, 4, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }

    memset(tmpData, 0, PAGE_SIZE);
    prepareApiColumnRecord(3, "index-position", TypeInt, 4, 5, tmpData);
    if (rbfm->insertRecord(columnFileHandle, columnRecordDescriptor, tmpData, dummyRid) < 0) {
        free(tmpData);
        rbfm->closeFile(columnFileHandle);
        return -1;
    }


    if (rbfm->closeFile(columnFileHandle) < 0) {
        free(tmpData);
        return -1;
    }

    // ==============   finished Columns ================

    if (rbfm->createFile(INDICES_FILE_NAME) < 0) {
        free(tmpData);
        return -1;
    }
    
    free(tmpData);
    lastTableId = 3;

    return 0;
}

RC RelationManager::deleteCatalog() {
    if(rbfm->destroyFile(TABLES_FILE_NAME) < 0) {
        return -1;
    }       
    if(rbfm->destroyFile(COLUMNS_FILE_NAME) < 0) {
        return -1;
    }

    if(rbfm->destroyFile(INDICES_FILE_NAME) < 0) {
        return -1;
    }
    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    
    lastTableId++;
    //printf("Creating table %s. Its table id is %d\n", tableName.c_str(), lastTableId);
    RID rid;

    // create a file for this table to store pages of records
    string fileName = tableName;
    if (rbfm->createFile(fileName) < 0) {
        return -1;
    }

    // prepare and insert table info into Tables
    int apiTableRecordSize = 0;

    void *data = malloc(PAGE_SIZE);
    prepareApiTableRecord(lastTableId, tableName, fileName, data, apiTableRecordSize);


    if (insertTuple(TABLES_TABLE_NAME, data, rid) < 0) {                                   
        free(data);
        return -1;
    }


    // prepare and insert record descriptor info into Columns
    Attribute attribute;
    for (int i = 0; i < attrs.size(); i++) {
        attribute = attrs[i];
        memset(data, 0, PAGE_SIZE);
        prepareApiColumnRecord(lastTableId, attribute.name, attribute.type, attribute.length, i + 1, data);

        //printf("inserting attribute to Columns:\n");
        //printTuple(columnRecordDescriptor, data);

        if (insertTuple(COLUMNS_FILE_NAME, data, rid) < 0) {
            free(data);
            return -1;
        }
    }

    //printf("############## recordDescriptor of %s is inserted to Columns\n", tableName.c_str());

    free(data);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName) {   
    if (strcmp(tableName.c_str(), TABLES_TABLE_NAME) == 0 || strcmp(tableName.c_str(), COLUMNS_TABLE_NAME) == 0|| !fexists(tableName)) {
        return -1;
    }

    if (rbfm->destroyFile(tableName) < 0) {
        return -1;
    }

    //printf("table file of %s is deleted, now deleting <id, tableName, fileName> from Tables\n", tableName.c_str());

    int tableId;
    RID rid;
    
    getTableIdByTableName(tableId, rid, tableName);

    FileHandle tableFileHandle;
    rbfm->openFile(TABLES_TABLE_NAME, tableFileHandle);


    //printf("Before delete in Tables\n");
    //rbfm->printTable(tableFileHandle, tableRecordDescriptor);

    if (rbfm->deleteRecord(tableFileHandle, tableRecordDescriptor, rid) < 0) {
        rbfm->closeFile(tableFileHandle);
        return -1;
    }

    //printf("After delete in Tables\n");
    //rbfm->printTable(tableFileHandle, tableRecordDescriptor);

    if (rbfm->closeFile(tableFileHandle) < 0) {
        return -1;
    }


    vector<string> attributeNames;
    attributeNames.push_back("table-id");
    RM_ScanIterator rm_ScanIterator;
    void *targetTuple = malloc(PAGE_SIZE);

    vector<string> attributeIndexNames;
    attributeNames.push_back("index-name");


    //search in "Columns" to find the tuples where table-id = tableId
    scan(COLUMNS_TABLE_NAME, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);

    //printf("rm::scan(Columns, table-id, EQ_OP, &tableId, attributeNames, rm_ScanIterator) done\n");


    //printf("Before delete in Columns\n");
    //rbfm->printTable(rm_ScanIterator.rbfm_ScanIterator.fileHandle, columnRecordDescriptor);


    while (rm_ScanIterator.getNextTuple(rid, targetTuple) != -1) {
        if (rbfm->deleteRecord(rm_ScanIterator.rbfm_ScanIterator.fileHandle, columnRecordDescriptor, rid) < 0) {
            if (DEBUG) printf("Error in delete, rmsi closed\n");
            rm_ScanIterator.close();
            free(targetTuple);
            return -1;
        }
    }

    //printf("After delete in Columns\n");
    //rbfm->printTable(rm_ScanIterator.rbfm_ScanIterator.fileHandle, columnRecordDescriptor);


    // search in "Indices" to find the tuples where table-id = tableId
    scan(INDICES_TABLE_NAME, "table-id", EQ_OP, &tableId, attributeIndexNames, rm_ScanIterator);

    while (rm_ScanIterator.getNextTuple(rid, targetTuple) != -1) {
        if (rbfm->deleteRecord(rm_ScanIterator.rbfm_ScanIterator.fileHandle, columnRecordDescriptor, rid) < 0) {
            if (DEBUG) printf("Error in delete, rmsi closed\n");
            rm_ScanIterator.close();
            free(targetTuple);
            return -1;
        }

        // for every index, delete from catalog, get it's attribute name, destroy index
        int lengthOfIndexName = *(int*)((char*)targetTuple + 1);
        char* indexName = (char*)malloc(lengthOfIndexName);
        indexName[lengthOfIndexName] ='\0';

        memcpy(indexName, (char*)targetTuple + 1 + sizeof(int), lengthOfIndexName);
        string indexNameStr(indexName);
        destroyIndex(tableName, indexNameStr);
    }

    rbfm->destroyFile(tableName);

    rm_ScanIterator.close();
    free(targetTuple);

    return 0;
}


// read recordDescriptor out from Columns
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {

    if (tableName.compare(TABLES_TABLE_NAME) == 0) {
        attrs = tableRecordDescriptor;
        return 0;
    }
    if (tableName.compare(COLUMNS_TABLE_NAME) == 0) {
        attrs = columnRecordDescriptor;
        return 0;
    } 


    int tableId = 0;
    RID rid;
    
    getTableIdByTableName(tableId, rid, tableName);

    vector<string> attributeNames;
    attributeNames.push_back("column-name");
    attributeNames.push_back("column-type");
    attributeNames.push_back("column-length");
    attributeNames.push_back("column-position");
    

    FileHandle fileHandle;

    if (rbfm->openFile(COLUMNS_FILE_NAME, fileHandle) < 0) {
        if (DEBUG) printf("Error in openFile: %s\n", COLUMNS_FILE_NAME);
        return -1;
    }

    RBFM_ScanIterator rbfm_ScanIterator;
    rbfm->scan(fileHandle, columnRecordDescriptor, "table-id", EQ_OP, &tableId, attributeNames, rbfm_ScanIterator);
    rbfm_ScanIterator.tableName = tableName;

    void *recordData = malloc(PAGE_SIZE);

    
    int attriLength;
    char* attriName = (char*)malloc(PAGE_SIZE);
    memset(attriName, 0, PAGE_SIZE);
    int attriNameLength;
    int position;
    AttrType attriType;
    int offset = 0;

    vector<AttributeWithPosition> attrWithPositions;
    AttributeWithPosition attrWithPosition;

    while (rbfm_ScanIterator.getNextRecord(rid, recordData) != -1) {
        Attribute attri;
        offset = 1;

        memcpy(&attriNameLength, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(attriName, (char*)recordData + offset, attriNameLength);
        offset += attriNameLength;

        memcpy(&attriType, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(&attriLength, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        memcpy(&position, (char*)recordData + offset, sizeof(int));
        offset += sizeof(int);

        attriName[attriNameLength] = '\0';
        attri.name = attriName;
        attri.type = attriType;
        attri.length = attriLength;

        attrWithPosition = {.attribute = attri, .position = position};
        attrWithPositions.push_back(attrWithPosition);
    }

    // sort attribute according to position
    sort(attrWithPositions.begin(), attrWithPositions.end(), CompLess());

    // extract the sorted attributes
    for (int i = 0; i < attrWithPositions.size(); i++) {
        attrs.push_back(attrWithPositions[i].attribute);
    }

    rbfm_ScanIterator.close();

    free(recordData);
    free(attriName);
    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }
    
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {   

    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);   

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }
    
    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) < 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    //insert index
    if (tableName!="Tables"&&tableName!="Columns"&&tableName!="Indices") {
        insertIndexWithInsertTuple(fileHandle, tableName, recordDescriptor, rid);
    }


    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    //printf("tuple is inserted into %s at RID(%d, %d):\n", tableName.c_str(), rid.pageNum, rid.slotNum);
    //printTuple(recordDescriptor, data);


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
        rbfm->closeFile(fileHandle);
        return -1;
    }

    // delete index accordingly
    deleteIndexWithDeleteTuple(fileHandle, tableName, recordDescriptor, rid);

    if (rbfm->closeFile(fileHandle) < 0) {
        return -1;
    }

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    getAttributes(tableName, recordDescriptor);

    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->updateRecord(fileHandle, recordDescriptor, data, rid) < 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    //first delete index
    deleteIndexWithDeleteTuple(fileHandle, tableName, recordDescriptor, rid);

    //then insert index
    insertIndexWithInsertTuple(fileHandle, tableName, recordDescriptor, rid);

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
        rbfm->closeFile(fileHandle);
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

    // find out target attribute index
    int targetAttrIndex = 0;
    for (; targetAttrIndex < recordDescriptor.size(); targetAttrIndex++) {
        if (recordDescriptor[targetAttrIndex].name.compare(attributeName) == 0) {
            break;
        }
    }
    if (targetAttrIndex == recordDescriptor.size()) {
        if(DEBUG) printf("conditionAttribute not found!\n");
        return -1;
    }


    // read out the inner record
    void *targetInnerRecord = malloc(PAGE_SIZE);
    if(rbfm->readInnerRecord(fileHandle, recordDescriptor, rid, targetInnerRecord) < 0) {
        if(DEBUG) printf("Error in readInnerRecord\n");
        free(targetInnerRecord);
        rbfm->closeFile(fileHandle);
        return -1;
    }

    // read out the attribute from the target inner record given the attrIndex   
    rbfm->readAttributeFromInnerRecord(recordDescriptor, targetInnerRecord, targetAttrIndex, data) ;


    free(targetInnerRecord);
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
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    FileHandle fileHandle;
    if (rbfm->openFile(tableName, fileHandle) < 0) {
        return -1;
    }

    if (rbfm->scan(fileHandle, recordDescriptor , conditionAttribute, compOp, value, attributeNames, 
        rm_ScanIterator.rbfm_ScanIterator) < 0) {
        return -1;
    }

    rm_ScanIterator.rbfm_ScanIterator.tableName = tableName;
    
    return 0;
}


// find tableId of tableName in Tables
void RelationManager::getTableIdByTableName(int &tableId, RID &rid, const string &tableName) {

    vector<string> attributeNames;
    attributeNames.push_back("table-id");

    //search in "Tables" to find the corresponding table-id of tableName
    FileHandle fileHandle;
    RBFM_ScanIterator rbfm_ScanIterator;
    rbfm->openFile(TABLES_FILE_NAME, fileHandle);

    // change tableName.c_str() in the format length|data

    int tableNameLength = strlen(tableName.c_str());
    void *tableNameInnerFormat = malloc(sizeof(int) + tableNameLength);
    *(int*)tableNameInnerFormat = tableNameLength;
    
    memcpy((char*)tableNameInnerFormat + sizeof(int), tableName.c_str(), tableNameLength);

    rbfm->scan(fileHandle, tableRecordDescriptor, "table-name", EQ_OP, tableNameInnerFormat, attributeNames, rbfm_ScanIterator);

    free(tableNameInnerFormat);

    rbfm_ScanIterator.tableName = tableName;

    void *apiTuple = malloc(PAGE_SIZE);

    rbfm_ScanIterator.getNextRecord(rid, apiTuple);
    //printf("In getTableIdByName. The record containing desired table-id is:\n");
    //rbfm->printRecord(rbfm_ScanIterator.projectedDescriptor, apiTuple);

    // skip the 1st byte, which is the nullIndicate byte
    tableId = *(int*)((char*)apiTuple + 1);
    rbfm_ScanIterator.close();
    free(apiTuple);
    rbfm->closeFile(fileHandle);

    //printf("table %s has a tableId of: %d\n", tableName.c_str(), tableId);            // ##### CORRECT !!!
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

void RelationManager::createIndicesRecordDescriptor(vector<Attribute> &recordDescriptor) {
    Attribute attr1;
    attr1.name = "table-id";
    attr1.type = TypeInt;
    attr1.length = (AttrLength)4;
    recordDescriptor.push_back(attr1);

    Attribute attr2;
    attr2.name = "index-name";
    attr2.type = TypeVarChar;
    attr2.length = (AttrLength)50;
    recordDescriptor.push_back(attr2);

    Attribute attr3;
    attr3.name = "index-type";
    attr3.type = TypeInt;
    attr3.length = (AttrLength)4;
    recordDescriptor.push_back(attr3);

    Attribute attr4;
    attr4.name = "index-length";
    attr4.type = TypeInt;
    attr4.length = (AttrLength)4;
    recordDescriptor.push_back(attr4);

    Attribute attr5;
    attr5.name = "index-position";
    attr5.type = TypeInt;
    attr5.length = (AttrLength)4;
    recordDescriptor.push_back(attr5);
}


//----------------------------------------------------------------------------------------------------


RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
    string ixname = tableName + "->" + attributeName;

    if (fexists(ixname)) {
        return -1;
    }

    void *data = malloc(PAGE_SIZE);

    int tableId;
    RID rid;
    getTableIdByTableName(tableId, rid, tableName);

    RM_ScanIterator rm_scanIterator;
    vector<string> attriname;
    attriname.push_back(attributeName);

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    Attribute attribute;
    for(int i = 0; i < recordDescriptor.size(); i++){
        if(recordDescriptor[i].name == attributeName){
            attribute = recordDescriptor[i];
        }
    }

    prepareApiColumnRecord(tableId, attribute.name, attribute.type, attribute.length, 0, data);
    insertTuple(INDICES_TABLE_NAME, data, rid);

    free(data);

    ixm->createFile(ixname);
    IXFileHandle ixFileHandle;
    ixm->openFile(ixname, ixFileHandle);

    void *key = malloc(PAGE_SIZE);

    this->scan(tableName, "", NO_OP, NULL, attriname, rm_scanIterator);
    while(rm_scanIterator.getNextTuple(rid, key) != EOF){
        assert(!(*(char*)key & (1 << 7))); // there is no NULL key
        ixm->insertEntry(ixFileHandle, attribute, (char*)key + 1, rid);
    }
    rm_scanIterator.close();
    ixm->closeFile(ixFileHandle);
    free(key);

    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    string ixname = tableName + "->" + attributeName;

    if (tableName == "Tables" || tableName == "Columns" || !fexists(ixname)) {
        return -1;
    }

    vector<string> attributes;
    string tableIdSingle="table-id";
    attributes.push_back(tableIdSingle);

    int ixnameLength = strlen(ixname.c_str());
    void *ixnameInnerFormat = malloc(ixnameLength + sizeof(int));
    *(int*)((char*)ixnameInnerFormat) = ixnameLength;
    memcpy((char*)ixnameInnerFormat + sizeof(int), ixname.c_str(), ixnameLength);

    RM_ScanIterator rm_ScanIterator;
    this->scan(INDICES_TABLE_NAME, "index-name", EQ_OP, ixnameInnerFormat, attributes, rm_ScanIterator);

    free(ixnameInnerFormat);

    RID rid;
    void *tupleWithTableId = malloc(PAGE_SIZE);
    rm_ScanIterator.getNextTuple(rid, tupleWithTableId);
    rm_ScanIterator.close();

    deleteTuple(INDICES_TABLE_NAME, rid);

    free(tupleWithTableId);
    rm_ScanIterator.close();
    rbfm->destroyFile(ixname);
    return 0;


}

RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator)
{
    string ixname = tableName + "->" + attributeName;

    //IndexManager *ixm = IndexManager::instance();

    IXFileHandle ixFileHandle;

    int rc = ixm->openFile(ixname, ixFileHandle);
    if(rc != 0){
        return -1;
    }

    vector<Attribute> recordDescriptor;
    this->getAttributes(tableName, recordDescriptor);
    Attribute attribute;
    for (int i = 0; i< recordDescriptor.size(); i++){
        if(strcmp(recordDescriptor[i].name.c_str(), attributeName.c_str()) == 0){
            attribute = recordDescriptor[i];
            break;
        }
    }

    return ixm->scan(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
}


void RelationManager::insertIndexWithInsertTuple(FileHandle &fileHandle, const string &tableName,
                                               vector<Attribute> &recordDescriptor, const RID &rid) {

    int tableId;
    RID ridTemp;
    getTableIdByTableName(tableId, ridTemp, tableName);

    IXFileHandle ixFileHandle;
    RM_ScanIterator rm_ScanIterator;
    RID ridIndex;

    vector<string> attributeIndexName;
    string indexName = "index-name";
    attributeIndexName.push_back(indexName);
    void *tupleIndex = malloc(PAGE_SIZE);

    scan(INDICES_TABLE_NAME, "table-id", EQ_OP, &tableId, attributeIndexName, rm_ScanIterator);

    while (rm_ScanIterator.getNextTuple(ridIndex, tupleIndex)!=-1) {
        // for every index ,get it's attri name, then get file name

        int lengthOfIndexName = *(int*)((char*)tupleIndex + 1);
        char* indexName = (char*)malloc(lengthOfIndexName + 1);
        indexName[lengthOfIndexName] = '\0';

        memcpy(indexName, (char*)tupleIndex + 1 + sizeof(int), lengthOfIndexName);
        string indexNameStr(indexName);
        string fileName;
        fileName = tableName + "->" + indexNameStr;


        ixm->openFile(fileName, ixFileHandle);

        Attribute attribute;
        for(int i = 0; i < recordDescriptor.size(); i++){
            if(strcmp(recordDescriptor[i].name.c_str(), indexNameStr.c_str()) == 0){
                attribute = recordDescriptor[i];
            }
        }

        void *key = malloc(PAGE_SIZE);
        //rbfm->openFile(tableName, fileHandle);
        rbfm->readAttribute(fileHandle, recordDescriptor, rid, indexNameStr, key);
        //rbfm->closeFile(fileHandle);

        ixm->insertEntry(ixFileHandle, attribute, (char*)key + 1, rid);
        free(key);
        free(indexName);
        ixm->closeFile(ixFileHandle);

    }

    rm_ScanIterator.close();
    free(tupleIndex);
}

void RelationManager::deleteIndexWithDeleteTuple(FileHandle &fileHandle, const string &tableName,
                                               vector<Attribute> &recordDescriptor, const RID &rid) {

    int tableId;
    RID ridTemp;
    getTableIdByTableName(tableId, ridTemp, tableName);

    IXFileHandle ixFileHandle;
    RM_ScanIterator rm_ScanIterator;
    RID ridIndex;

    vector<string> attributeIndexName;
    string indexName = "index-name";
    attributeIndexName.push_back(indexName);
    void *tupleIndex = malloc(PAGE_SIZE);

    scan("Indices", "table-id", EQ_OP, &tableId, attributeIndexName, rm_ScanIterator);

    while (rm_ScanIterator.getNextTuple(ridIndex, tupleIndex) != -1) {
        // for every index ,get it's attri name, then get file name

        int lengthOfIndexName = *(int*)((char*)tupleIndex + 1);
        char* indexName = (char*)malloc(lengthOfIndexName + 1);
        indexName[lengthOfIndexName] ='\0';

        memcpy(indexName, (char*)tupleIndex + 1 + sizeof(int), lengthOfIndexName);
        string indexNameStr(indexName);
        string fileName;
        fileName = tableName + "->" + indexNameStr;


        Attribute attribute;
        for(int i = 0; i < recordDescriptor.size(); i++){
            if(recordDescriptor[i].name == indexNameStr){
                attribute = recordDescriptor[i];
            }
        }

        void *key = malloc(PAGE_SIZE);
        //rbfm->openFile(tableName, fileHandle);
        rbfm->readAttribute(fileHandle, recordDescriptor, rid, indexNameStr, key);
        //rbfm->closeFile(fileHandle);

        ixm->openFile(fileName, ixFileHandle);
        ixm->deleteEntry(ixFileHandle, attribute, (char*)key + 1, rid);
        free(key);
        free(indexName);
        ixm->closeFile(ixFileHandle);

    }

    rm_ScanIterator.close();
    free(tupleIndex);
}




