#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>

#include "../rbf/pfm.h"

using namespace std;

//  Record format
//  number of fields: short, 2 bytes
//  null indicator: n bytes
//  fields:
//      case 1: type == string
//          int length + "ABCD\n": so 4 + str.length + 1 bytes
//      case 2: type == int / float
//          4 bytes only
//
// free space
//
// offset(2 bytes) + 
// Record ID

typedef struct
{
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
    string   name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};


// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0, // no condition// = 
           LT_OP,      // <
           LE_OP,      // <=
           GT_OP,      // >
           GE_OP,      // >=
           NE_OP,      // !=
           NO_OP       // no condition
} CompOp;

class RBFM_ScanIterator;


class RecordBasedFileManager {
public:
    static RecordBasedFileManager* instance();
    
    // ====== start of self defined methods =======
    // ==== start of project 1 ===
    RC getNullIndicatorSize(const int fieldCount); 

    RC composeInnerRecord(const vector<Attribute> &recordDescriptor, const void *data, void *tmpRecord, short &size);

    Page *initializePage(const unsigned pageNum);

    RC findInsertLocation(FileHandle &fileHandle, const short recordSize, RID &rid, short &offset);

    void readSlotFromPage(Page *page, const short slotNum, Slot &slot);

    void appendSlotToPage(Page *page, const short slotNum, const Slot &slot);

    void writeSlotToPage(Page *page, const short slotNum, const Slot &slot);

    void readRecordFromPage(Page *page, const short offset, const short recordSize, void *data);

    RC readInnerRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    void appendInnerRecordToPage(Page *page, const short offset, const void *record, const short recordSize);

    int getIntData(int offset, const void* data);

    float getFloatData(int offset, const void* data);
    
    RC getVarCharData(int offset, const void* data, char* varChar, const int varCharLength);
    // ==== end of project 1 ===

    // ==== start of project 2 ===
    void shiftBytes(char *start, int length, int delta);

    RC composeApiTuple(const vector<Attribute> &recordDescriptor, vector<int> &projectedDescriptor, 
        void *innerRecord, void *tuple, short &size);


    void printInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord);

    void printTable(FileHandle fileHandle, const vector<Attribute> &recordDescriptor);
    // ==== end of project 2 ===


    // ====== end of self defined methods ========

    RC createFile(const string &fileName);
  
    RC destroyFile(const string &fileName);
  
    RC openFile(const string &fileName, FileHandle &fileHandle);
  
    RC closeFile(FileHandle &fileHandle);

//  Format of the data passed into the function is the following:
//  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
//  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
//     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
//     Each bit represents whether each field value is null or not.
//     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
//     If k-th bit from the left is set to 0, k-th field contains non-null values.
//     If there are more than 8 fields, then you need to find the corresponding byte first, 
//     then find a corresponding bit inside that byte.
//  2) Actual data is a concatenation of values of the attributes.
//  3) For Int and Real: use 4 bytes to store the value;
//     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
//  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
// For example, refer to the Q8 of Project 1 wiki page.
RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

// This method will be mainly used for debugging/testing. 
// The format is as follows:
// field1-name: field1-value  field2-name: field2-value ... \n
// (e.g., age: 24  height: 6.1  salary: 9000
//        age: NULL  height: 7.5  salary: 7500)
RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);


/******************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for the part 1 of the project
******************************************************************************************************************************************************************/
RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

// Assume the RID does not change after an update
RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);


void readAttributeFromInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord, const int conditionAttrIndex, void *data);

// Scan returns an iterator to allow the caller to go through the results one by one. 
RC scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator);

public:

protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

private:
    static RecordBasedFileManager *_rbf_manager;
};


/********************************************************************************
The scan iterator is NOT required to be implemented for the part 1 of the project 
********************************************************************************/


# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RBFM_ScanIterator {

public:
    
    bool opened;

    FileHandle fileHandle;
    
    // next record id
    RID nextRid;

    // predicate
    int conditionAttrIndex;
    CompOp op;
    void *value;

    vector<Attribute> recordDescriptor;
    vector<Attribute> projectedDescriptor;
    vector<int> projectedDescriptorIndex;


public:
    RBFM_ScanIterator() {

      opened = false;
      nextRid.pageNum = 0;
      nextRid.slotNum = 0;

      conditionAttrIndex = -1;
      op = NO_OP;
      value = NULL;

      recordDescriptor.clear();
      projectedDescriptor.clear();
      projectedDescriptorIndex.clear();
    };
    ~RBFM_ScanIterator() {};

    // Never keep the results in the memory. When getNextRecord() is called, 
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);
    RC close();

    // self defined functions
    bool opCompare(void* ref1, void* ref2, CompOp op, AttrType type);
};




#endif
