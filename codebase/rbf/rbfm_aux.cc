#include "rbfm.h"
#include <cstring>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>


int RecordBasedFileManager::getNullIndicatorSize(const int fieldCount) { 
    return ceil(fieldCount / 8.0);
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


void RecordBasedFileManager::readSlotFromPage(Page *page, const short slotNum, Slot &slot) {
    memcpy(&slot, (char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), sizeof(Slot));
}


void RecordBasedFileManager::writeSlotToPage(Page *page, const short slotNum, 
        const Slot &slot) {
    memcpy((char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), 
            &slot, sizeof(Slot));
    page->header.slotCount += 1;
    page->header.freeSpace -= sizeof(Slot);
}


void RecordBasedFileManager::readRecordFromPage(Page *page, const short offset, const short recordSize, void *data) {
    memcpy(data, (char*)page + offset, recordSize);
}


void RecordBasedFileManager::insertRecordToPage(Page *page, const short offset, const void* record, 
        const short recordSize) {
    memcpy((char*)page + offset, record, recordSize);
    page->header.recordCount += 1;
    page->header.freeSpace -= recordSize;
    page->header.freeSpaceOffset += recordSize;
}



// return an initialized page 
Page* RecordBasedFileManager::initializePage(const unsigned pageNum) {
    Page *tmpPage = new Page;
    tmpPage->header = {.pageNumber = pageNum, .recordCount = 0, .slotCount = 0, 
        .freeSpace = DATA_SIZE, .freeSpaceOffset = HEADER_SIZE};
    return tmpPage;    
}



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


// given recordSize, locate the page with enough free space to store the record
// offset is modified to: starting position of the page to insert the record
// rid is modified properly
RC RecordBasedFileManager::findInsertLocation(FileHandle &fileHandle, const short recordSize, RID &rid, short &offset) {
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
        Page *newPage = initializePage(totalPage);
        curHeader = newPage->header;
        fileHandle.appendPage(newPage);
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


RC RecordBasedFileManager::readInnerRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, 
        const RID &rid, const void *data) {

    // validate pageNum
    unsigned totalPageNum = fileHandle.getNumberOfPages();
    if (rid.pageNum >= totalPageNum) {
        //printf("page to be read does not exist!\n");
        return -1;
    }

    // read page
    Page *page = new Page;
    if (fileHandle.readPage(rid.pageNum, page) < 0) {
        //printf("Error in fileHandle.readPage\n");
        return -1;
    }
    short slotCount = page->header.slotCount;


    // validate slotNum
    if (rid.slotNum >= slotCount) {
        //printf("slot to be read does not exist!\n");
        return -1;
    }

    // read slot
    Slot targetSlot = {};
    readSlotFromPage(page, rid.slotNum, targetSlot);


    // case 1: the record is delete
    if (targetSlot.offset == -1) {
        //printf("Record to be read is deleted\n");
        return -1;
    }

    // case 2: the record is redirected to another page, trace to that page
    if (targetSlot.length == -1) {
        RID targetRid = *(RID*)((char*)page + targetSlot.offset);
        free(page);
        return readInnerRecord(fileHandle, recordDescriptor, targetRid, data);
    }

    // case 3: the record is on this page
    // read out the target record
    void *targetInnerRecord = malloc(PAGE_SIZE);
    readRecordFromPage(page, targetSlot.offset, targetSlot.length, targetInnerRecord);
    free(page);
    return 0;
}


// compose an inner record given an api record with null indicator
//   inner record format:
//      short: fieldCount
//      short [filedCount]: field offset, -1 if it's null
//      field values
RC RecordBasedFileManager::composeInnerRecord(const vector<Attribute> &recordDescriptor, const void *data, 
    void *tmpRecord, short &size) {

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



RC RecordBasedFileManager::composeApiTuple(const vector<Attribute> &recordDescriptor, vector<int> &projectedDescriptor, 
        void *innerRecord, void *tuple, short &size) {

    int fieldCount = projectedDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    short tupleOffset = nullIndicatorSize;


    // initialize null indicator bytes
    void *nullIndicator = malloc(nullIndicatorSize);
    memset(nullIndicator, 0, nullIndicatorSize);

    int bitPos = 7;
    for (int projectedIndex = 0; projectedIndex < projectedDescriptor.size(); projectedIndex++) {
        int nullByteNum = projectedIndex / 8;
        int bitMask = 1 << bitPos;            
        // shift bit position to the right by 1 for next iteration
        bitPos--;
        bitPos = (CHAR_BIT + bitPos) % CHAR_BIT;

        short fieldOffset = *(short*)((char*)innerRecord + sizeof(short) * (1 + projectedDescriptor[projectedIndex]));
        
        // case 1: null field
        if (fieldOffset == -1) {
            ((char*)nullIndicator)[nullByteNum] |= bitMask;
            continue;
        }

        // case 2: valid field
        Attribute fieldAttr = recordDescriptor[projectedDescriptor[projectedIndex]];
        switch(fieldAttr.type) {
            case TypeInt: {
                int intData = getIntData(fieldOffset, innerRecord);
                *(int*)((char*)tuple + tupleOffset) = intData;
                tupleOffset += sizeof(int);
                break;
            }
            case TypeReal: {
                float floatData = getFloatData(fieldOffset, innerRecord);
                *(float*)((char*)tuple + tupleOffset) = floatData;
                tupleOffset += sizeof(float);
                break;
            }
            case TypeVarChar: {
                int varCharLength = *(int*)((char*)innerRecord + fieldOffset);
                memcpy((char*)tuple + tupleOffset, (char*)innerRecord + fieldOffset, sizeof(int) + varCharLength);
                tupleOffset += sizeof(int) + varCharLength;
                break;
            }
            default: {
                break;
            }
        }
    }

    size = tupleOffset;
    // write null indicator into tuple
    memcpy(tuple, nullIndicator, nullIndicatorSize);
    free(nullIndicator);
    return 0;
}



RC RecordBasedFileManager::readAttributeFromInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord, 
        const int conditionAttrIndex, void *data) {

    // find the offset for the desired attribute
    short attrOffset = *(short*)((char*)innerRecord + (1 + conditionAttrIndex) * sizeof(short));
    if (recordDescriptor[conditionAttrIndex].type == TypeVarChar) {
        int varCharLen = *(int*)((char*)innerRecord + attrOffset);
        memcpy(data, (char*)innerRecord + attrOffset + sizeof(int), varCharLen);
        ((char*)data)[varCharLen] = '\0';
    } else {
        memcpy(data, (char*)innerRecord + attrOffset, sizeof(int));
    }
    return 0;
}



void RecordBasedFileManager::printInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord) {
    int fieldCount = recordDescriptor.size();
    int offset = (fieldCount + 1) * sizeof(short);
    for (int i = 0; i < fieldCount; i++) {
        printf("%s\t", recordDescriptor[i].name.c_str());
        if (*(short*)((char*)innerRecord + (1 + i) * sizeof(short)) == -1) {
            printf("NULL\n");
        }
        switch(recordDescriptor[i].type) {
            case TypeVarChar: {
                int varCharLength = *(int*)((char*)innerRecord + offset);
                char varChar[varCharLength + 1];
                getVarCharData(offset + sizeof(int), innerRecord, varChar, varCharLength);
                offset += varCharLength;
                printf("%s\n", varChar);
                break;
            }
            case TypeInt: {
                printf("%d\n", getIntData(offset, innerRecord));
                break;
            }
            case TypeReal: {
                printf("%.2f\n", getFloatData(offset, innerRecord));
                break;
            }
            default: 
                printf("\n");
                break;
        }
        offset += sizeof(int);
    }
}




