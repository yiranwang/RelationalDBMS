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

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance()->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance()->closeFile(fileHandle);
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

    short insertOffset = -1;  // location on the page to insert the record
    short recordSize = -1;
    char *innerRecord = (char*)malloc(PAGE_SIZE);
    // get innerRecord, recordSize
    if (composeInnerRecord(recordDescriptor, data, innerRecord, recordSize) < 0) {
        delete innerRecord;
        return -1;
    }

    // get rid, offset
    findInsertLocation(fileHandle, recordSize, rid, insertOffset);
    // read out the destination page
    Page *page = new Page;
    if (fileHandle.readPage(rid.pageNum, page) < 0) {
        return -1;
    }
    // append the inner record onto page, header adjusted
    appendInnerRecordToPage(page, insertOffset, innerRecord, recordSize);
    
    // append the slot onto page, header adjusted
    Slot slot = {.offset = insertOffset, .length = recordSize, .isRedirected = 0}; 
    appendSlotToPage(page, rid.slotNum, slot); 

    // write page into file
    if (fileHandle.writePage(rid.pageNum, page) < 0) {
        return -1;
    }

    // free the allocated memory
    free(innerRecord);
    delete page;
    return 0;
}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);
   
    // read the slot 
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);

    // check if a record is inside the current page
    // case 1: record is deleted
    if (slot.offset < 0) {
    	delete page;
        if (DEBUG) printf("error: this record is deleted\n");
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

    // should have put below codes into a function: composeApiFromInnerRecord

    // get field count (short)
    short fieldCount = *(short*)innerRecord;
    if (fieldCount != (short)(recordDescriptor.size())) {
        //printf("error, fieldCount != recordDescriptor.size()\n");
        return -1;
    } 
    // initialize offset in record and data
    short recordOffset = sizeof(short) * (1 + fieldCount);
    int nullBytesNum = getNullIndicatorSize((int)fieldCount); 
    short dataOffset = nullBytesNum;


    // write inner record out to data in Api format
    int bitPos = CHAR_BIT - 1;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
        short fieldOffset = *(short *)(innerRecord + (fieldIndex + 1) * sizeof(short));

        // check if this field is NULL
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


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Null-indicators initialization
    bool nullBit = false;
    int fieldCount = recordDescriptor.size();
    int nullIndicatorSize = getNullIndicatorSize(fieldCount);
    int offset = nullIndicatorSize;
    // print each field name; print its value if not null, otherwise print NULL
    int bitPos = CHAR_BIT - 1;
    for (int fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++) {
            printf("%s\t", recordDescriptor[fieldIndex].name.c_str());
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


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {

    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);


    // read slot of the record to be deleted
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);

    // check if a record is inside the current page                                                 
    // case 1: record is deleted already
    if (slot.offset < 0) {                                                                          
        delete page;
        return 0;
    }
    // case 2: record is redirected to another page                                                 
    // because after being updated, the current page cannot hold it                                 
    if (slot.length < 0) {
        RID nextRid = *(RID*)((char*)page + slot.offset);              

        // mark this slot as deleted and make it persistent
        slot.offset = -1;
        writeSlotToPage(page, rid.slotNum, slot);
        fileHandle.writePage(rid.pageNum, page);

        delete page;
        return deleteRecord(fileHandle, recordDescriptor, nextRid);
    }

    // ============ normal case ================

    // case 3: slot is valid, record-to-be-deleted is on this page
    short lastSlotNum = page->header.slotCount - 1; 

    // if deleting the very last record, no need to shift anything
    // otherwise shift every following record to the left
    if (rid.slotNum < lastSlotNum) {

        // get the size of the rest records first
        short restSize = page->header.freeSpaceOffset - slot.offset - slot.length;  

        // shift the rest records to the LEFT by slot.length


        shiftBytes((char*)page + slot.offset + slot.length, restSize, 0 - slot.length);
        // adjust offset of the rest records' slots
        for (short slotIndex = (short)rid.slotNum + 1; slotIndex <= lastSlotNum; slotIndex++) {

            Slot curSlot = {};
            readSlotFromPage(page, slotIndex, curSlot);
            if (curSlot.offset < 0) {
                continue;
            }

            curSlot.offset -= slot.length;
            writeSlotToPage(page, slotIndex, curSlot); 
        }
    }

    // adjust freeSpace and freeSpaceOffset
    page->header.freeSpace += slot.length;
    page->header.freeSpaceOffset -= slot.length;
    // set slot.offset to -1 to indicate that it's deleted
    slot.offset = -1;
    // write the updated slot to page and updated page back to file
    writeSlotToPage(page, rid.slotNum, slot);
    if(fileHandle.writePage(rid.pageNum, page) < 0) {
        delete page;
        return -1;
    }

    delete page; 
    return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {

    // read the page
    Page *page = new Page;
    fileHandle.readPage(rid.pageNum, page);

    // read slot
    Slot slot = {};
    readSlotFromPage(page, rid.slotNum, slot);

    // check if a record is inside the current page                                                 
    // case 1: record is deleted already
    if (slot.offset < 0) {                                                                          
        delete page;
        return -1;
    }
    // case 2: record is redirected to another page                                                 
    // because after being updated, the current page cannot hold it                                 
    else if (slot.length < 0) {
        RID nextRid = *(RID*)((char*)page + slot.offset);                                           
        delete page;
        return updateRecord(fileHandle, recordDescriptor, data, nextRid);
    }



    // case 3: slot is valid, record-to-be-deleted is on this page
    // first compose the updated inner record and get its size
    short updatedRecordSize = -1;
    char *updatedInnerRecord = (char*)malloc(PAGE_SIZE);
    if (composeInnerRecord(recordDescriptor, data, updatedInnerRecord, updatedRecordSize) < 0) {
        delete page;
        return -1;
    }
    // find the record size change and the rest records' size:
    short delta = updatedRecordSize - slot.length; 
    short restSize = page->header.freeSpaceOffset - slot.offset - slot.length;  
    short shiftAmount = 0;


    // case 3.1: the page has enough space to hold the updated record
    // store the updated record at the original position and adjust the rest
    if (page->header.freeSpace >= delta) {
        shiftAmount = delta;
        shiftBytes((char*)page + slot.offset + slot.length, restSize, shiftAmount);  
        memcpy((char*)page + slot.offset, updatedInnerRecord, updatedRecordSize); 
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
        shiftBytes((char*)page + slot.offset + slot.length, restSize, shiftAmount);
        // insert the update record and get targetRid
        RID targetRid = {};
        if (insertRecord(fileHandle, recordDescriptor, data, targetRid) < 0) {
            delete page;
            free(updatedInnerRecord);
            return -1;
        }

        // mark the slot pointed by targetRid as isRecirected
        Slot tmpSlot = {};
        Page* tmpPage = new Page;
        fileHandle.readPage(targetRid.pageNum, tmpPage);
        readSlotFromPage(tmpPage, targetRid.slotNum, tmpSlot);
        tmpSlot.isRedirected = 1;
        writeSlotToPage(tmpPage, targetRid.slotNum, tmpSlot);
        fileHandle.writePage(targetRid.pageNum, tmpPage);
        delete tmpPage;

        // fill the RID of target page on this page and set slot.length = -1
        RID *pointingRid = (RID*)((char*)page + slot.offset);
        pointingRid->pageNum = targetRid.pageNum;
        pointingRid->slotNum = targetRid.slotNum;
        slot.length = -1;


        printf("This page cannot hold the updated record, it's moved from RID(%u, %u) to RID(%u, %u)\n", rid.pageNum, rid.slotNum, targetRid.pageNum, targetRid.slotNum);
    } 


    // adjust offset of the rest records' slots
    for (int slotIndex = rid.slotNum + 1; slotIndex < page->header.slotCount; slotIndex++) {
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










