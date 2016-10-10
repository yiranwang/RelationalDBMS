#include "rbfm.h"
#include <cstring>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, 
        FileHandle &fileHandle) {
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
//   record format:
//      short: fieldCount
//      short [filedCount]: field offset, -1 if it's null
//      field values
RC RecordBasedFileManager::composeRecord(
        const vector<Attribute> &recordDescriptor, 
        const void *data, void *tmpRecord, short &size) {
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    short recordOffset = 0;
    short dataOffset = nullIndicatorSize;
    
    // first 2 bytes in a record is the fieldCount of short type
    *(short*)tmpRecord = fieldCount;
    // skip (fieldCount + 1) shorts to the starting address of the fields 
    recordOffset += sizeof(short) * (1 + fieldCount);
    int bitPos = 7;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            int nullByteNum = fieldIndex / 8;
            int bitMask = 1 << bitPos;            
            // shift bit position to the right by 1 for next iteration
            bitPos--;
            bitPos = (CHAR_BIT + bitPos) % CHAR_BIT;
            nullBit = ((char*)data)[nullByteNum] & bitMask;
            if (!nullBit) {
                // write field offset
                *(short*)((char*)tmpRecord + sizeof(short) * (1 + fieldIndex)) = 
                    recordOffset;
            
                // write field value
                Attribute fieldAttr = recordDescriptor[fieldIndex];
                if (fieldAttr.type == TypeVarChar) {
                    // get varChar length
                    int varCharLen = *(int*)((char*)data + dataOffset);
                    memcpy((char*)tmpRecord + recordOffset, 
                            (char*)data + dataOffset, sizeof(int) + varCharLen);

                    // move offset in data and record for next field
                    dataOffset += sizeof(int) + varCharLen;
                    recordOffset += sizeof(int) + varCharLen;
                } else {
                    memcpy((char*)tmpRecord + recordOffset, 
                            (char*)data + dataOffset, sizeof(int));
                    dataOffset += sizeof(int);
                    recordOffset += sizeof(int);
                }
                
            } else {
                // for a null field, write -1 to its field offset
                *(short*)((char*)tmpRecord + sizeof(short) * (1 + fieldIndex)) = 
                    -1;
            }
    }
    size = recordOffset;
    return 0;
}

// return an initialized page 
Page RecordBasedFileManager::initializePage(const unsigned pageNum) {
    Page tmpPage = {};
    tmpPage.header = {.pageNumber = pageNum, .recordCount = 0, .slotCount = 0, 
        .freeSpace = DATA_SIZE, .freeSpaceOffset = HEADER_SIZE};
    return tmpPage;    
}

// given recordSize, locate the page with enough free space to store the record
// offset is modified to: starting position of the page to insert the record
// rid is modified properly
RC RecordBasedFileManager::findInsertLocation(FileHandle &fileHandle, 
        const short recordSize, RID &rid, short &offset) {
    // get total number of pages 
    int totalPage = (int)fileHandle.getNumberOfPages();
    short recordSlotSize = recordSize + sizeof(Slot);
    // start from the last page
    int targetPageNum = totalPage - 1;
    PageHeader curHeader = {}; 
    for (; targetPageNum >= 0 && targetPageNum < 2 * totalPage - 1; 
            targetPageNum++) {
        int actualPageNum = targetPageNum % totalPage;// n - 1, 0, 1, ..., n -2
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
        rid.slotNum = curHeader.slotCount;
    }
    offset = curHeader.freeSpaceOffset;
    return 0;
}

RC RecordBasedFileManager::readSlotFromPage(Page *page, const short slotNum, 
        Slot &slot) {
    memcpy(&slot, (char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), 
            sizeof(Slot));
    return 0;
}

RC RecordBasedFileManager::writeSlotToPage(Page *page, const short slotNum, 
        const Slot &slot) {
    memcpy((char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), 
            &slot, sizeof(Slot));
    page->header.slotCount += 1;
    page->header.freeSpace -= sizeof(Slot);
    return 0;
}

RC RecordBasedFileManager::insertRecordToPage(Page *page, const short offset, 
        const void* record, const short recordSize) {
    memcpy((char*)page + offset, record, recordSize);
    page->header.recordCount += 1;
    page->header.freeSpace -= recordSize;
    page->header.freeSpaceOffset += recordSize;
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, 
        const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    short insertOffset = -1;  // location on the page to insert the record
    short recordSize = -1;
    char *tmpRecord = (char*)malloc(PAGE_SIZE);
    // get tmpRecord, recordSize
    if (composeRecord(recordDescriptor, data, tmpRecord, recordSize) < 0) {
        if (DEBUG) {
            return -1;
        }
        delete tmpRecord;
        return -1;
    }
    if(DEBUG) {
        printf("compose done: size=%d\n", recordSize);
    }
    // get rid, offset
    findInsertLocation(fileHandle, recordSize, rid, insertOffset);
    if(DEBUG) {
        printf("find location done: offset=%d\n", insertOffset);
    }
    // read out the destination page
    Page *page = new Page;
    if (fileHandle.readPage(rid.pageNum, page) < 0) {
        if (DEBUG) {
            printf("failed to read page out of file\n");
        }
    }
    if(DEBUG) {
        printf("page read out done: pageNum=%d\n", rid.pageNum);
    }
    // write record onto page
    if (insertRecordToPage(page, insertOffset, tmpRecord, recordSize)) {
        if (DEBUG) {
            printf("failed to write record to page\n");
        }
    }
    if(DEBUG) {
        printf("insert record to page done\n");
    }
    // write slot onto page
    Slot slot = {.offset = insertOffset, .length = recordSize}; 
    if (writeSlotToPage(page, rid.slotNum, slot) < 0) {
        if(DEBUG) {
            printf("failed to write slot to page\n");
        }
        return -1;
    }        
    if(DEBUG) {
        printf("write slot done\n");
    }
    // write page into file
    if (fileHandle.writePage(rid.pageNum, page) < 0) {
        if (DEBUG) {
            printf("failed to write page to file\n");
        }
    }
    if(DEBUG) {
        printf("write to file done\n");
    }
    // free the allocated memory
    free(tmpRecord);
    delete page;
    if(DEBUG) {
        printf("free tmpRecord and page pointer done\n");
    }
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, 
        const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);
    if(DEBUG) {
        printf("read page from file done\n");
    }
    // read the slot 
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);
    if(DEBUG) {
        printf("read slot done\n");
    }
    // check if a record is inside the current page
    // case 1: record is deleted
    if (slot.offset < 0) {
    	delete page;
    	if(DEBUG) {
            printf("The record to be read has been deleted!\n");
        }
        return -1;
    }
    // case 2: record is redirected to another page
    // because after being updated, the current page cannot hold it
    else if (slot.length < 0) {
    	RID nextRid = *(RID*)((char*)page + slot.offset);
    	delete page;
    	return readRecord(fileHandle, recordDescriptor, nextRid, data);
    }
    // case 3: normal case, the record is inside the current page
    // read the record inner format 
    char* innerRecord = (char*)malloc(PAGE_SIZE);
    memcpy(innerRecord, (char*)page + slot.offset, slot.length);
    if(DEBUG) {
        printf("copy inner record done\n");
    } 
    // get field count (short)
    short fieldCount = *(short*)innerRecord;
    if (fieldCount != (short)recordDescriptor.size()) {
    	if(DEBUG) {
            printf("Error: fieldCount != recordDescriptor.size()\n");
        }return -1;
    } 
    // initialize offset in record and data
    short recordOffset = sizeof(short) * (1 + fieldCount);
    int nullBytesNum = getNullIndicatorSize((int)fieldCount); 
    short dataOffset = nullBytesNum;
    // write inner record out to data
    int bitPos = CHAR_BIT - 1;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
        short fieldOffset = 
            *(short *)(innerRecord + (fieldIndex + 1) * sizeof(short));
        if (fieldOffset >= 0) {
            Attribute fieldAttr = recordDescriptor[fieldIndex];     
            if (fieldAttr.type == TypeVarChar) {
                int varCharLen = *(int*)(innerRecord + recordOffset);
                memcpy((char*)data + dataOffset, innerRecord + recordOffset, 
                        sizeof(int) + varCharLen);
                dataOffset += varCharLen;
                recordOffset += varCharLen;
            } else {
                memcpy((char*)data + dataOffset, 
                        innerRecord + recordOffset, sizeof(int));
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
    delete page;
    return 0;
}

RC RecordBasedFileManager::printRecord(
        const vector<Attribute> &recordDescriptor, const void *data) {
    // Null-indicators initialization
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    int offset = nullIndicatorSize;
    // print each field name; print its value if not null, otherwise print NULL
    int bitPos = CHAR_BIT - 1;
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
                        getVarCharData(offset + sizeof(int), data, 
                                varChar, varCharLength);
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

int RecordBasedFileManager::getIntData(int offset, const void* data) {
    int intData;
    memcpy(&intData, (char *)data + offset, sizeof(int));
    return intData;
}

float RecordBasedFileManager::getFloatData(int offset, const void* data) {
    float floatData;
    memcpy(&floatData, (char *)data + offset, sizeof(float));
    return floatData;
}

RC RecordBasedFileManager::getVarCharData(int offset, const void* data, 
        char* varChar, const int varCharLength) {
    memcpy(varChar, (char *)data + offset, varCharLength);
    varChar[varCharLength] = '\0';
    return 0;
}

//  ========== start of project 2 methods ============

/*  Shift [start, start + length - 1] by delta bytes
 *  delta > 0: shift right, delta < 0: shift left
 *  delta == 0: no shift
 */
void RecordBasedFileManager::shiftBytes(char *start, int length, int delta) {
    if (delta == 0 || length == 0) {
        return;
    }
    // starting address of shifted piece 
    char* dest = start + delta;
    // shift left: copy 0, 1, 2, ..., n-1
    if (delta < 0) {
        for (int i = 0; i < length; i++) {
            dest[i] = start[i];
        }
    } 
    // shift right: copy n-1, n-2, ..., 2, 1, 0
    else {
        for (int i = length - 1; i >= 0; i--) {
            dest[i] = start[i];
        }
    }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, 
        const vector<Attribute> &recordDescriptor, const RID &rid) {
    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);
    if(DEBUG) {
        printf("read page from file done\n");
    } 
    // read slot of the record to be deleted
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);
    if(DEBUG) {
        printf("read slot done\n");                                                                
    }
    // check if a record is inside the current page                                                 
    // case 1: record is deleted already
    if (slot.offset < 0) {                                                                          
        delete page;
        if(DEBUG) {
            printf("The record to be deleted has been deleted!\n");                              
        }
        return 0;
    }
    // case 2: record is redirected to another page                                                 
    // because after being updated, the current page cannot hold it                                 
    else if (slot.length < 0) {
        RID nextRid = *(RID*)((char*)page + slot.offset);                                           
        delete page;
        return deleteRecord(fileHandle, recordDescriptor, nextRid);
    }
    // case 3: slot is valid, record-to-be-deleted is in this page
    short lastSlotNum = page->header.slotCount - 1; 
    // if deleting the very last record, no need to shift anything
    // otherwise shift every following record to the left
    if (rid.slotNum < lastSlotNum) {
        // get the size of the rest records first
        int restSize = page->header.freeSpaceOffset - slot.offset - slot.length;  
        // shift the rest records to the LEFT by slot.length
        shiftBytes((char*)page + slot.offset + slot.length, 
                restSize, 0 - slot.length);
        if (DEBUG) {
            printf("delete the record from this page done\n");
        }
    
        // adjust offset of the rest records' slots
        for (int slotIndex = rid.slotNum + 1; 
                slotIndex < page->header.slotCount; slotIndex++) {
            Slot curSlot = {};
            readSlotFromPage(page, slotIndex, curSlot);
            curSlot.offset -= slot.length;
            writeSlotToPage(page, slotIndex, curSlot); 
        }
    }
    // adjust freeSpace and freeSpaceOffset
    page->header.freeSpace += slot.length;
    page->header.freeSpaceOffset -= slot.length;
    // set slot.offset to -1 to indicate that it's deleted
    slot.offset = -1;
    // write changes of the page to file
    writeSlotToPage(page, rid.slotNum, slot);
    if(fileHandle.writePage(rid.pageNum, page) < 0) {
        if (DEBUG) {
            printf("failed to write page into file\n");
        }
        return -1;
    }
    delete page; 
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, 
        const vector<Attribute> &recordDescriptor, 
        const void *data, const RID &rid) {
    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);
    if(DEBUG) {
        printf("read page from file done\n");
    } 
    // read slot
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);
    if(DEBUG) {
        printf("read slot done\n");                                                                
    }
    // check if a record is inside the current page                                                 
    // case 1: record is deleted already
    if (slot.offset < 0) {                                                                          
        delete page;
        if(DEBUG) {
            printf("The record to be updated has been deleted!\n");                              
        }
        return -1;
    }
    // case 2: record is redirected to another page                                                 
    // because after being updated, the current page cannot hold it                                 
    else if (slot.length < 0) {
        RID nextRid = *(RID*)((char*)page + slot.offset);                                           
        delete page;
        return updateRecord(fileHandle, recordDescriptor, data, nextRid);
    }
    // case 3: slot is valid, record-to-be-deleted is in this page
    // first compose the updated inner record and get its size
    short updatedRecordSize = -1;
    char *updatedInnerRecord = (char*)malloc(PAGE_SIZE);
    if (composeRecord(recordDescriptor, data, updatedInnerRecord, 
                updatedRecordSize) < 0) {
        if (DEBUG) {
            printf("failed to compose record\n");
        }
        delete page;
        return -1;
    }
    // find the record size change and the rest records' size:
    int delta = (int)(updatedRecordSize - slot.length); 
    int restSize = page->header.freeSpaceOffset - slot.offset - slot.length;  
    int shiftAmount = 0;
    // case 3.1: the page has enough space to hold the updated record
    // store the updated record at the original position and adjust the rest
    if (page->header.freeSpace >= delta) {
        shiftAmount = delta;
        shiftBytes((char*)page + slot.offset + slot.length, 
                restSize, shiftAmount);  
        memcpy((char*)page + slot.offset, updatedInnerRecord, 
                updatedRecordSize); 
        page->header.freeSpace -= delta;
        page->header.freeSpaceOffset += delta;
        slot.length = updatedRecordSize;
    }
    // case 3.2: the page doesn't have enough space to hold the updated record
    // replace the record on this page with an RID of the target page
    // store the updated record on the target page
    // updatedRecord object is not used afterwards
    else {
        // replace record with RID
        // shift rest records to the LEFT by slot.length - sizeof(RID)
        shiftAmount = sizeof(RID) - slot.length;
        shiftBytes((char*)page + slot.offset + slot.length,
                restSize, shiftAmount);
        // insert the update record and get targetRid
        RID targetRid = {};
        if (insertRecord(fileHandle, recordDescriptor, data, targetRid) < 0) {
            if (DEBUG) {
                printf("failed to insert record\n");
            }
            delete page;
            free(updatedInnerRecord);
            return -1;
        }
        // fill the RID of target page on this page and set slot.length = -1
        RID *pointingRid = (RID*)((char*)page + slot.offset);
        pointingRid->pageNum = targetRid.pageNum;
        pointingRid->slotNum = targetRid.slotNum;
        slot.length = -1;
    } 
    // adjust offset of the rest records' slots
    for (int slotIndex = rid.slotNum + 1; 
            slotIndex < page->header.slotCount; slotIndex++) {
        Slot curSlot = {};
        readSlotFromPage(page, slotIndex, curSlot);
        curSlot.offset += shiftAmount;
        writeSlotToPage(page, slotIndex, curSlot); 
    }
    // make changes persistent and free memory
    writeSlotToPage(page, rid.slotNum, slot);
    fileHandle.writePage(rid.pageNum, page);
    delete page;
    free(updatedInnerRecord);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, 
        const vector<Attribute> &recordDescriptor, const RID &rid, 
        const string &attributeName, void *data) {
    void* record = malloc(PAGE_SIZE);    
    memset(record, 0, PAGE_SIZE);
    // read record out 
    if (readRecord(fileHandle, recordDescriptor, rid, record) < 0) {
        if (DEBUG) {
            printf("Read record failed\n");
        }
        return -1;
    }
    if (DEBUG) {
        printf("read record out done\n");
    }
    // find the index of the target attribute
    int attrIndex = 0;
    for (; strcmp(recordDescriptor[attrIndex].name.c_str(), 
                attributeName.c_str()) != 0 
            && attrIndex < recordDescriptor.size(); attrIndex++) {
    }
    if (attrIndex == recordDescriptor.size()) {
        if (DEBUG) {
            printf("attribute not found\n");
        }
        free(record);
        return -1;
    }
    if(DEBUG) {
        printf("attrIndex found for %s:%d\n", attributeName.c_str(), attrIndex);
    }
    // find the offset for the attribute
    short attrOffset = 
        *(short*)((short*)record + (1 + attrIndex) * sizeof(short)); 
    if (attrOffset == -1) {
        if (DEBUG) {
            printf("attribute: %s is NULL\n", attributeName.c_str());
        }
        free(record);
        return -1;
    }
    if (recordDescriptor[attrIndex].type == TypeVarChar) {
        int varCharLen = *(int*)((char*)record + attrOffset);
        memcpy(data, (char*)record + attrOffset, sizeof(int) + varCharLen);
    } else {
        memcpy(data, (char*)record + attrOffset, sizeof(int));
    }
    free(record);
    return 0;
}

//  ========== end of project 2 methods ============
