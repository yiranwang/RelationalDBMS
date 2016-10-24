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


RC RecordBasedFileManager::getVarCharData(int offset, const void* data, char* varChar, const int varCharLength) {
    memcpy(varChar, (char *)data + offset, varCharLength);
    varChar[varCharLength] = '\0';
    return 0;
}


void RecordBasedFileManager::readSlotFromPage(Page *page, const short slotNum, Slot &slot) {
    memcpy(&slot, (char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), sizeof(Slot));
}


void RecordBasedFileManager::writeSlotToPage(Page *page, const short slotNum, const Slot &slot) {
    memcpy((char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));
}


void RecordBasedFileManager::appendSlotToPage(Page *page, const short slotNum, const Slot &slot) {
    memcpy((char*)page + PAGE_SIZE - (slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));
    page->header.slotCount += 1;
    page->header.freeSpace -= sizeof(Slot);
}


void RecordBasedFileManager::readRecordFromPage(Page *page, const short offset, const short recordSize, void *data) {
    memcpy(data, (char*)page + offset, recordSize);
}


void RecordBasedFileManager::appendInnerRecordToPage(Page *page, const short offset, const void* record, const short recordSize) {
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
        //printf("shifting left by %d...\n", 0 - delta);
        for (int i = 0; i < length; i++) {
            dest[i] = start[i];
        }
    } 
    // shift right: copy n-1, n-2, ..., 2, 1, 0
    else {
        //printf("Shifting right by %d...\n", delta);
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


    //printf("Location found:RID(%d, %d)\n", rid.pageNum, rid.slotNum);
    return 0;
}


RC RecordBasedFileManager::readInnerRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

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

    //printf("Inside readInnerRecord, the record is on this page!\n");
    // case 3: the record is on this page
    // read out the target record
    readRecordFromPage(page, targetSlot.offset, targetSlot.length, data);
    free(page);
    return 0;
}


/* compose an inner record (tmpRecord) given an api record (data) with null indicator
//   inner record format:
//      short: fieldCount
//      short [filedCount]: field offset, -1 if it's null
//      field values
*/

RC RecordBasedFileManager::composeInnerRecord(const vector<Attribute> &recordDescriptor, const void *data, void *tmpRecord, short &size) {

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



RC RecordBasedFileManager::composeApiTuple(const vector<Attribute> &recordDescriptor, vector<int> &projectedDescriptor, void *innerRecord, void *tuple, short &size) {

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



// data will have 1 nullIndicator byte for this only one attribute
void RecordBasedFileManager::readAttributeFromInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord, const int targetAttrIndex, void *data) {

    // initialize nullIndicator byte as: 00000000
    memset(data, 0, 1);
   
    // find the offset for the desired attribute
    short attrOffset = *(short*)((char*)innerRecord + (1 + targetAttrIndex) * sizeof(short));

    // if attribute's value is NULL
    if (attrOffset < 0) {
        // set the first bit in the nullIndicator byte: 10000000 = 128
        *(unsigned char*) data = 128;
        //printf("The attribute %s has a value of NULL\n", recordDescriptor[targetAttrIndex].name.c_str());
    }
    else if (recordDescriptor[targetAttrIndex].type == TypeVarChar) {
        int varCharLen = *(int*)((char*)innerRecord + attrOffset);
        memcpy((char*)data + 1, (char*)innerRecord + attrOffset + sizeof(int), varCharLen);
        ((char*)data)[varCharLen + 1] = '\0';
        //printf("Read off %s = %s\n", recordDescriptor[targetAttrIndex].name.c_str(), ((char*)data + 1));
    } 
    else if (recordDescriptor[targetAttrIndex].type == TypeInt) {
        memcpy((char*)data + 1, (char*)innerRecord + attrOffset, sizeof(int));
        //printf("Read off %s = %d\n", recordDescriptor[targetAttrIndex].name.c_str(), *(int*)((char*)data + 1));
    } else {
        memcpy((char*)data + 1, (char*)innerRecord + attrOffset, sizeof(float));
        //printf("Read off %s = %f\n", recordDescriptor[targetAttrIndex].name.c_str(), *(float*)((char*)data + 1));
    }
}


// print the inner record, used for debug purpose
void RecordBasedFileManager::printInnerRecord(const vector<Attribute> &recordDescriptor, void *innerRecord) {
    short fieldCount = *(short*)innerRecord;;

    if (fieldCount != (short)recordDescriptor.size()) {
        printf("Error in printInnerRecord: fieldCount = %d from this innerRecord doesn't match recordDescriptor.size() = %lu!\n", fieldCount, recordDescriptor.size());
        return;
    }

    printf("%d attributes in this records\n", *(short*)innerRecord);
    for (int i = 0; i < fieldCount; i++) {
        printf("@offset: %d\t|\t%s = ", *(short*)((char*)innerRecord + sizeof(short) * (1 + i)), recordDescriptor[i].name.c_str());

        short fieldOffset = *(short*)((char*)innerRecord + (1 + i) * sizeof(short));
        if (fieldOffset == -1) {
            printf("NULL\t");
            continue;
        }
        switch(recordDescriptor[i].type) {
            case TypeVarChar: {
                int varCharLength = *(int*)((char*)innerRecord + fieldOffset);
                char varChar[varCharLength + 1];
                getVarCharData(fieldOffset + sizeof(int), innerRecord, varChar, varCharLength);
                printf("%s\t|\t", varChar);
                break;
            }
            case TypeInt: {
                printf("%d\t|\t", getIntData(fieldOffset, innerRecord));
                break;
            }
            case TypeReal: {
                printf("%.2f\t|\t", getFloatData(fieldOffset, innerRecord));
                break;
            }
            default: 
                printf("Error\t|\t");
                break;
        }
    }
}


void RecordBasedFileManager::printTable(FileHandle fileHandle, const vector<Attribute> &recordDescriptor) {
    
    unsigned totalPageNum = fileHandle.getNumberOfPages();
    printf("There are %u pages in this file\n", totalPageNum);
    Page *page = new Page;
    char innerRecord [PAGE_SIZE];


    for (unsigned i = 0; i < totalPageNum; i++) {

        printf("Reading page %u...\n", i);

        fileHandle.readPage(i, page);
        short slotCount = page->header.slotCount;
        printf("There are %d slots on this page\n", slotCount);

        printf("######################## Page %d ################################################\n", i);

        for (short j = 0; j < slotCount; j++) {
            RID rid = {.pageNum = (unsigned)i, .slotNum = (unsigned)j};
            

            Slot slot = {};
            readSlotFromPage(page, j, slot);
            if (slot.offset == -1 || slot.length < 0) {
                continue;
            }

            readInnerRecord(fileHandle, recordDescriptor, rid, innerRecord);

            printf("RID(%d, %d): occupis %d bytes in disk\n", i, j, *(short*)innerRecord);

            printInnerRecord(recordDescriptor, innerRecord);
            printf("\n========================================================================================\n");
        }

    }

    delete page;
}




