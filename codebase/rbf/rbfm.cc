#include "rbfm.h"
#include <iostream>
#include <math.h>
#include <algorithm>

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
    return PagedFileManager::instance() -> createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance() -> destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance() -> openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return PagedFileManager::instance() -> closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    
    //|fieldCount|offset1|offset2|..|offsetn||field1|field2|...|fieldn|

    void *pagePointer = malloc(PAGE_SIZE);

    void *recordPointer = malloc(PAGE_SIZE);

    //------

    int nullIndicatorSize = ceil((double)recordDescriptor.size() / 8);

    short fieldNum = recordDescriptor.size(); 

    int offset = 0;
    int offsetRecord = 0;

    //fill fieldCount(short)
    *((short*) ((char*)recordPointer + offsetRecord)) = fieldNum;

    //offset in record move right
    offsetRecord += sizeof(short);

    //unsigned char *nullIndicator = (unsigned char*)recordPointer + offsetRecord;

    //copy nullIndicatorField in data to record
    //memcpy((char*)recordPointer + offsetRecord, (char*)data, nullIndicatorSize);
    //offsetRecord += nullIndicatorSize;

    unsigned char* nullIndicator = (unsigned char*)malloc(nullIndicatorSize);
    memcpy(nullIndicator, (char*)data, nullIndicatorSize);

    offset += nullIndicatorSize;

    //offsetRecord move to begin of field
    offsetRecord += sizeof(short) * fieldNum;

    int fieldNum2 = fieldNum;

    for (int i = 0; i < nullIndicatorSize; i++) {

        int upperBound = fieldNum2 <= 8 ? fieldNum2 : 8;

        bool isNull = false;

        for (int j = 0; j < upperBound; j++) {

            Attribute record = recordDescriptor[i * 8 + j];

            //whether this field is null according to the nullIndicator
            isNull = nullIndicator[i] & (1 << (7 - j));

            //cout<<    "insert isNull is"<< isNull <<endl;

            if (!isNull) {

                if (record.type == TypeInt) {

                    memcpy((char*)recordPointer + offsetRecord, (char*)data + offset, sizeof(int));

                    //int value= *((int*)((char*)recordPointer + offsetRecord));
                    //cout<< "value is"<< value <<endl;

                    offset += record.length;
                    offsetRecord += record.length;

                }else if (record.type == TypeReal) {

                    memcpy((char*)recordPointer + offsetRecord, (char*)data + offset, sizeof(float));

                    //float value= *((float*)((char*)data + offset));
                    //cout<< "value is"<< value <<endl;

                    offset += record.length;
                    offsetRecord += record.length;

                }else if (record.type == TypeVarChar) {

                    //get the length of varchar
                    int length = *((int*)((char*)data + offset));
                    offset += sizeof(int);

                    memcpy((char*)recordPointer + offsetRecord, (char*)data + offset, length);

                    /*
                    string name((char*)recordPointer + offsetRecord, length);

                    cout << recordDescriptor[i].name << "name:" << name << "\t";

                    name.~string();*/

                    offset += length;
                    offsetRecord += length;
                }
            }

            //write the according offset to each field after the nullIndicator
            *((short*)recordPointer + (i * 8 + j + 1)) = offsetRecord;
        }
        if (fieldNum2 >= 8) {

            fieldNum2 -= 8;
        }else {
            break;
        }
        
    }

    //cout<<    "offsetRecord is"<< offsetRecord <<endl;

    //|record1|record2|...|recordn||freespace|slot1|slot2|...|slotn|numofRecordInPage|freeOffSet|---

    int totalPage = fileHandle.getNumberOfPages();

    int offsetFree = 0; ////
    int recordNumInPage = 0;

    int i = 0;

    for (i = 0; i < totalPage; i++) {

        if (i == 0) {
            // put record to last page first
            fileHandle.readPage(totalPage - 1, pagePointer);

        }else {

            fileHandle.readPage(i - 1, pagePointer);
        }

        //fileHandle.readPage(i, pagePointer);

        offsetFree = *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int)));

        recordNumInPage = *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 2));

        int freeSpaceSize = PAGE_SIZE - offsetRecord - sizeof(int) - offsetFree - sizeof(int) * (recordNumInPage + 2);
        //int freeSpaceSize = PAGE_SIZE - offsetRecord - offsetFree - sizeof(int) * (recordNumInPage + 2);

        if (freeSpaceSize > 0) {

            //copy record to page
            memcpy((char*)pagePointer + offsetFree, recordPointer, offsetRecord);

            //store record offsetFree in half of slot
            *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * (recordNumInPage + 3))) = (short)offsetFree;

            //store record length in another half of slot
            *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * (recordNumInPage + 3) + sizeof(short))) = (short)offsetRecord;

            offsetFree += offsetRecord;

            recordNumInPage++;

            *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int))) = offsetFree;

            *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 2)) = recordNumInPage;

            //fileHandle.writePage(i, pagePointer);
            if (i == 0) {

                fileHandle.writePage(totalPage - 1, pagePointer);

                rid.pageNum = totalPage - 1;

            }else {

                fileHandle.writePage(i - 1, pagePointer);

                rid.pageNum =i - 1;
            }

            rid.slotNum = recordNumInPage - 1;

            break;
        } 
    }

    //write to a new page

    if (i == totalPage) {

        memcpy((char*)pagePointer, recordPointer, offsetRecord);

        *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 3)) = 0;

        *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 3 + sizeof(short))) = (short)offsetRecord;

        *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int))) = offsetRecord;

        *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 2)) = 1;

        fileHandle.appendPage(pagePointer);

        rid.pageNum = fileHandle.getNumberOfPages() - 1;

        rid.slotNum = 0;
    }

    free(recordPointer);

    free(pagePointer);

    free(nullIndicator);
    return 0;

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

    void* pagePointer = malloc(PAGE_SIZE);

    void* recordPointer = pagePointer;

    fileHandle.readPage(rid.pageNum, pagePointer);

    //int offsetFree = *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int)));

    //int recordNumInPage = *((int*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * 2));

    int offsetRecord = *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * (rid.slotNum + 3)));

    //int recordLength = *((short*)((char*)pagePointer + PAGE_SIZE - sizeof(int) * (rid.slotNum + 3) + sizeof(short)));

    //cout<<    "offsetRecord is"<< offsetRecord <<endl;
    //cout<<    "recordLength is"<< recordLength <<endl;

    //memcpy((char*)data, (char*)pagePointer + offsetRecord + sizeof(short), recordLength - sizeof(short));


    recordPointer = (char*)pagePointer + offsetRecord;

    short fieldNum = *((short*)recordPointer);
    //cout<< "fieldNum is"<< fieldNum <<endl;

    int nullIndicatorSize = ceil((double)recordDescriptor.size() / 8);

    unsigned char *nullIndicator = (unsigned char*)malloc(nullIndicatorSize);

    memset(nullIndicator, 0, nullIndicatorSize);

    int offset = nullIndicatorSize;

    for (int i = 0; i < fieldNum; i++) {

        Attribute record = recordDescriptor[i];

        bool isNull = false;

        int varLength = 0;

        //read each offset of field
        int fieldOffset = *((short*)recordPointer + (i + 1));

        if (i) {
            varLength = fieldOffset - *((short*)recordPointer + i);

        }else {
            varLength = fieldOffset - sizeof(short) * (fieldNum + 1);
        }

        if (varLength == 0) {
            isNull = true;

            nullIndicator[i / 8] |= (1 << (7 - i));
        }

        if (!isNull) {

            if (record.type == TypeInt) {

                memcpy((char*)data + offset, (char*)recordPointer + fieldOffset - sizeof(int), sizeof(int));

                //int value= *((int*)((char*)data + offset));
                //cout<< "read value is"<< value <<endl;

                offset += record.length;

            }else if (record.type == TypeReal) {

                memcpy((char*)data + offset, (char*)recordPointer + fieldOffset - sizeof(float), sizeof(float));

                //float value= *((float*)((char*)data + offset));
                //cout<< "float value is"<< value <<endl;

                offset += record.length;
                
            }else if (record.type == TypeVarChar) {

                *((int*)((char*)data + offset)) = varLength;

                offset += sizeof(int);

                memcpy((char*)data + offset, (char*)recordPointer + fieldOffset - varLength, varLength);

                //string name((char*)data + offset, varLength);
                //cout << recordDescriptor[i].name << "name:" << name << endl;
                //name.~string();

                offset += varLength;
                //cout << recordDescriptor[i].name << "varLength:" << varLength << endl;
            }
        }

    }

    //copy null indicator to data
    memcpy((char*)data, nullIndicator, nullIndicatorSize);

    free(nullIndicator);
    free(pagePointer);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    
    int nullIndicatorSize = ceil((double)recordDescriptor.size() / 8);

    unsigned char *nullIndicator = (unsigned char*)malloc(nullIndicatorSize);

    memcpy(nullIndicator, (char*)data, nullIndicatorSize);

    int offset = nullIndicatorSize;

    for (int i = 0; i < recordDescriptor.size(); i++) {

        Attribute record = recordDescriptor[i];

        int index = (double)i / 8;

        bool isNull = nullIndicator[index] & (1 << (7 - i));

        if (record.type == TypeInt) {

            if (!isNull) {

                cout << recordDescriptor[i].name << ":" << *((int*)((char*)data + offset)) << endl;

                offset += record.length;

            }else {
                cout << recordDescriptor[i].name << ":NULL" << endl;
            }
        }else if (record.type == TypeReal) {

            if (!isNull) {

                cout << recordDescriptor[i].name << ":" << *((float*)((char*)data + offset)) << endl;

                offset += record.length;

            }else {
                cout << recordDescriptor[i].name << ":NULL" << endl;
            }

        }else if (record.type == TypeVarChar) {

            if (!isNull) {

                int varLength = *((int*)((char*)data + offset));
                
                offset += sizeof(int);

                string name((char*)data + offset, varLength);

                cout << recordDescriptor[i].name << ":" << name << endl;

                offset += varLength;

            }else {
                cout << recordDescriptor[i].name << ":NULL" << endl;
            }

        }
    }

    free(nullIndicator);

    return 0;
}
