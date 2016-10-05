#include "rbfm.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}

// get # of bytes of null indicator 
int RecordBasedFileManager::getNullIndicatorSize(const int fieldCount) { 
    return ceil(fieldCount / 8.0);
}

// compose a record given a data with null indicator
// record format:
// short: fieldCount
// short[filedCount]: field offset, -1 if it's null
// field values
RC RecordBasedFileManager::composeRecord(const vector<Attribute> &recordDescriptor, const void *data, void *tmpRecord, short &size) {
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    short recordOffset = 0;
    short dataOffset = nullIndicatorSize;
    
    // first 2 bytes in a record is the fieldCount of short type
    *(short*)tmpRecord = fieldCount;
    // skip (fieldCount + 1) shorts to the starting address of the record fields 
    recordOffset += sizeof(short) * (1 + fieldCount);

    int bitPos = 7;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            int nullByteNum = fieldIndex / 8;
            int bitMask = 1 << bitPos;            
            // shift bit position of the bit mask right by 1 for next iteration
            bitPos--;
            bitPos = (CHAR_BIT + bitPos) % CHAR_BIT;

            nullBit = ((char*)data)[nullByteNum] & bitMask;
            if (!nullBit) {

                // write field offset
                *(short*)((char*)tmpRecord + sizeof(short) * (1 + fieldIndex)) = recordOffset;
            
                // write field value
                Attribute fieldAttr = recordDescriptor[fieldIndex];
                if (fieldAttr.type == TypeVarChar) {
                    // get varChar length
                    int varCharLen = *(int*)((char*)data + dataOffset);
                    memcpy((char*)tmpRecord + recordOffset, (char*)data + dataOffset, sizeof(int) + varCharLen);

                    // move offset in data and record for next field
                    dataOffset += sizeof(int) + varCharLen;
                    recordOffset += sizeof(int) + varCharLen;
                } else {
                    memcpy((char*)tmpRecord + recordOffset, (char*)data + dataOffset, sizeof(int));
                    dataOffset += sizeof(int);
                    recordOffset += sizeof(int);
                }
                
            } else {
                // for a null field, write -1 to its field offset
                *(short*)((char*)tmpRecord + sizeof(short) * (1 + fieldIndex)) = -1;
            }

    }

    size = recordOffset;
    return 0;
}

// return an initialized page 
Page RecordBasedFileManager::initializePage(const unsigned pageNum) {
    Page tmpPage = {};
    tmpPage.header = {.pageNumber = pageNum, .recordCount = 0, .freeSpace = DATA_SIZE, .offset = HEADER_SIZE};
    return tmpPage;    
}

// given recordSize, search for the page with enough free space to store the record
// offset is modified to: starting position of the page to insert the record
// rid is modified properly
RC RecordBasedFileManager::findInsertLocation(FileHandle &fileHandle, const short recordSize, RID &rid, short &offset) {
    // get total number of pages 
    int totalPage = (int)fileHandle.getNumberOfPages();
    short recordSlotSize = recordSize + sizeof(Slot);

    int targetPageNum = totalPage - 1;
    PageHeader curHeader = {}; 
    for (; targetPageNum >= 0 && targetPageNum < 2 * totalPage - 1; targetPageNum++) {
        int actualPageNum = targetPageNum % totalPage;              // n - 1, 0, 1, 2, ..., n -2
        fileHandle.readPageHeader(actualPageNum, &curHeader);
        if (curHeader.freeSpace >= recordSlotSize) {
            break;
        }
    }
    // case 1: empty file or no page can fit this record, append a new page
    // store this record as the 1st record on this page 
    if  (targetPageNum < 0 || targetPageNum == 2 * totalPage - 1) {
        Page newPage = initializePage(totalPage);
        curHeader = newPage.header;
        fileHandle.appendPage(&newPage);
        rid.pageNum = totalPage;
        rid.slotNum = 0;
    }
    // case 2: insert this record to an exisitng page which can hold it 
    else {
        rid.pageNum = targetPageNum % totalPage;
        rid.slotNum = curHeader.recordCount;
    }
    offset = curHeader.offset;

    return 0;
}


RC RecordBasedFileManager::readSlotFromPage(const Page &page, const short slotNum, Slot &slot) {
    memcpy(&slot, (char*)(&page) + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), sizeof(Slot));
    return 0;
}


RC RecordBasedFileManager::writeSlotToPage(Page &page, const short slotNum, const Slot &slot) {
    memcpy((char*)(&page) + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));
    page.header.freeSpace -= sizeof(Slot);
    return 0;
}


RC RecordBasedFileManager::insertRecordToPage(Page &page, const short offset, const void* record, const short recordSize) {
    memcpy((char*)(&page) + offset, record, recordSize);
    page.header.recordCount += 1;
    page.header.freeSpace -= recordSize;
    page.header.offset += recordSize;
    return 0;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    short offset = -1;
    short recordSize = -1;
    char *tmpRecord = (char*)malloc(PAGE_SIZE);

    // get tmpRecord, recordSize
    composeRecord(recordDescriptor, data, tmpRecord, recordSize);
    if(DEBUG) printf("compose done: size=%d\n", recordSize);

    // get rid, offset
    findInsertLocation(fileHandle, recordSize, rid, offset);
    if(DEBUG) printf("find location done: offset=%d\n", offset);

    // read out the destination page
    Page page = {};
    fileHandle.readPage(rid.pageNum, &page); 
    if(DEBUG) printf("page read out done: pageNum=%d\n", rid.pageNum);

    // write record onto page
    insertRecordToPage(page, offset, tmpRecord, recordSize);
    if(DEBUG) printf("insert record to page done\n");
    
    // write slot onto page
    Slot slot = {.offset = offset, .length = recordSize}; 
    writeSlotToPage(page, rid.slotNum, slot);
    if(DEBUG) printf("write slot done\n");

    // write page into file
    fileHandle.writePage(rid.pageNum, &page);
    if(DEBUG) printf("write to file done\n");

    free(tmpRecord);
    if(DEBUG) printf("free done\n");
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // read the page
    Page page = {};
    fileHandle.readPage(rid.pageNum, &page);

    // read the slot 
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);
    
    // read the record inner format 
    char* innerRecord = (char*)malloc(PAGE_SIZE);
    memcpy(innerRecord, (char*)(&page) + slot.offset, slot.length); 
    
    // get field count (short)
    short fieldCount = *(short*)innerRecord;
    if (fieldCount != (short)recordDescriptor.size()) {
        return -1;
    } 

    // initialize offset in record and data
    short recordOffset = sizeof(short) * (1 + fieldCount);
    int nullBytesNum = getNullIndicatorSize((int)fieldCount); 
    short dataOffset = nullBytesNum;

    // write inner record out to data
    int bitPos = CHAR_BIT - 1;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
        
        short fieldOffset = *(short *)(innerRecord + (fieldIndex + 1) * sizeof(short));
        if (fieldOffset >= 0) {
            Attribute fieldAttr = recordDescriptor[fieldIndex];     
            if (fieldAttr.type == TypeVarChar) {
                int varCharLen = *(int*)(innerRecord + recordOffset);
                memcpy((char*)data + dataOffset, innerRecord + recordOffset, sizeof(int) + varCharLen);
                dataOffset += varCharLen;
                recordOffset += varCharLen;
            } else {
                memcpy((char*)data + dataOffset, innerRecord + recordOffset, sizeof(int));
            }
            dataOffset += sizeof(int);
            recordOffset += sizeof(int);
        } else {
            int nullByteNum = fieldIndex / 8;
            int bitMask = 1 << bitPos;                
            ((char*)data)[nullByteNum] |= bitMask;            
        }
        bitPos--;
        bitPos = (CHAR_BIT + bitPos) % CHAR_BIT;
    }


    return 0;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
 
    // Null-indicators initialization
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    int offset = nullIndicatorSize;

    // print each field name; print its value if it's not null, otherwise print null
    int bitPos = 7;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            
            int nullByteNum = fieldIndex / 8;
            int bitMask = 1 << bitPos;            
            
            nullBit = ((char*)data)[nullByteNum] & bitMask;
            
            if (nullBit) {
               printf("NULL\n");            
            } else {
                Attribute fieldAttr = recordDescriptor[fieldIndex];
                switch(fieldAttr.type) {
                    case TypeInt: {
                        int intData = getIntData(offset, data);
                        printf("%d\n", intData);
                        break;
                    }
                    case TypeReal: {
                        float floatData = getFloatData(offset, data);
                        printf("%.2f\n", floatData);
                        break;
                    }
                    case TypeVarChar: {
                        int varCharLength = *(int*)((char*)data + offset);
                        char varChar[varCharLength + 1];
                        getVarCharData(offset + sizeof(int), data, varChar, varCharLength);
                        printf("%s\n", varChar);
                        offset += varCharLength;
                        break;
                    }
                    default: {
                        break;
                    }
                }
                offset += sizeof(int);
            }
            bitPos--;
            bitPos = (CHAR_BIT + bitPos) % CHAR_BIT;
    }
  
    return 0;
}


int RecordBasedFileManager::getIntData(int offset, const void* data)
{
    int intData;
    memcpy(&intData, (char *)data + offset, sizeof(int));
    return intData;
}


float RecordBasedFileManager::getFloatData(int offset, const void* data)
{
    float floatData;
    memcpy(&floatData, (char *)data + offset, sizeof(float));
    return floatData;
}


RC RecordBasedFileManager::getVarCharData(int offset, const void* data, char* varChar, const int varCharLength)
{
    memcpy(varChar, (char *)data + offset, varCharLength);
    varChar[varCharLength] = '\0';
    return 0;
}
