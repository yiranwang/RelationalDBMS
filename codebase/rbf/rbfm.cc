#include "rbfm.h"
#include <math.h>

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

RC RecordBasedFileManager::getNullIndicatorSize(const int fieldCount) { 
    // get # of bytes of null indicator 
    return (int)ceil(fieldCount / 8.0);
}


short RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) { 
    short recordSize = 0;
    // Null-indicators initialization
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    int offset = nullIndicatorSize;
   
    // Process each field
    int bitPos = 7;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {                    
        int nullByteNum = (int)(ceil)(fieldIndex / 8.0);
        int bitMask = 1 << bitPos;            
        
        nullBit = ((char*)data)[nullByteNum] & bitMask;
        if (!nullBit) {
            if (recordDescriptor[fieldIndex].type == TypeVarChar) {
                int varCharLength = *(int*)((char*)data + offset);   
                offset += sizeof(int) + varCharLength;
                recordSize += sizeof(int) + varCharLength;
            } else {
                offset += sizeof(int);
                recordSize += sizeof(int); 
            }
        }
    }
    return recordSize;
}


Page RecordBasedFileManager::initializePage(FileHandle &fileHandle, const unsigned pageNum) {
    Page tmpPage = {};
    tmpPage.header = {.pageNumber = pageNum, .recordCount = 0, .freeSpace = DATA_SIZE, .offset = HEADER_SIZE};
    return tmpPage;    
}


RC RecordBasedFileManager::findInsertLocation(FileHandle &fileHandle, const short recordSize, RID &rid, short &offset) {
    // get total number of pages 
    unsigned totalPage = fileHandle.getNumberOfPages();
    int recordSlotSize = recordSize + sizeof(Slot); 
    // search one by one by reading header. Need to improve in the future. 
    int targetPageNum = 0;
    PageHeader curHeader = {}; 
    for (; targetPageNum < totalPage; targetPageNum++) {
        fileHandle.readPageHeader(targetPageNum, &curHeader);
        if (curHeader.freeSpace >= recordSlotSize) {
            break;
        }
    }
    // case 1: no page fit this record, append a new page
    // store this record as the 1st record on this page 
    if  (targetPageNum == totalPage) {
        Page tmpPage = initializePage(fileHandle, totalPage);
        curHeader = tmpPage.header;
        fileHandle.appendPage(&tmpPage);
        rid.pageNum = totalPage;
        rid.slotNum = 0;
    }
    // case 2: insert this record to an exisitng page which can hold it 
    else {
        rid.pageNum = targetPageNum;
        rid.slotNum = curHeader.recordCount;
    }
    offset = curHeader.offset;
    return 0;
}


RC RecordBasedFileManager::readSlotFromPage(const Page &page, const short slotNum, Slot &slot) {
    memcpy(&slot, (char*)(&page) + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), sizeof(Slot));
    return 0;
}


RC RecordBasedFileManager::insertSlotToPage(Page &page, const short slotNum, const Slot &slot) {
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
    short recordSize = getRecordSize(recordDescriptor, data);
    findInsertLocation(fileHandle, recordSize, rid, offset);
    Page page = {};
    fileHandle.readPage(rid.pageNum, &page); 
    Slot slot = {.offset = offset, .length = recordSize}; 

    insertRecordToPage(page, offset, data, recordSize);

    insertSlotToPage(page, rid.slotNum, slot);
    
    fileHandle.writePage(rid.pageNum, &page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    Page page = {};
    fileHandle.readPage(rid.pageNum, &page);
    
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);
    memcpy(data, (char*)&page + slot.offset, slot.length);
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
                        
            int nullByteNum = (int)(ceil)(fieldIndex / 8.0);
            int bitMask = 1 << bitPos;            
            
            nullBit = ((char*)data)[nullByteNum] & bitMask;
            printf("%s\t", recordDescriptor[fieldIndex].name.c_str());
            if (nullBit) {
               printf("NULL\n");            
            } else {
                switch(recordDescriptor[fieldIndex].type) {
                    case TypeInt: {
                        int intData = getIntData(offset, data);
                        offset += sizeof(int);
                        printf("%d\n", intData);
                        break;
                    }
                    case TypeReal: {
                        float floatData = getFloatData(offset, data);
                        offset += sizeof(float);
                        printf("%.2f\n", floatData);
                        break;
                    }
                    case TypeVarChar: {
                        int varCharLength = *(int*)((char*)data + offset);
                        char varChar[varCharLength + 1];
                        getVarCharData(offset, data, varChar, varCharLength);
                        printf("%s\n", varChar);
                        offset += sizeof(int) + varCharLength;
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }
            bitPos = (8 + (--bitPos)) % 8;
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
    memcpy((void *)varChar, (char *)data + offset + sizeof(int), varCharLength);
    varChar[varCharLength] = '\0';
    return 0;
}
