#include "rbfm.h"
#include <cstring>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>



RC RecordBasedFileManager::scan(FileHandle &fileHandle, 
		const vector<Attribute> &recordDescriptor, 
		const string &conditionAttribute, 
		const CompOp compOp,                  // comparision type such as "<" and "="
      	const void *value,                    // used in the comparison
      	const vector<string> &attributeNames, // a list of projected attributes
      	RBFM_ScanIterator &rbfm_ScanIterator) {

	rbfm_ScanIterator.opened = true;
	rbfm_ScanIterator.fileHandle = fileHandle;


	int conditionIndex = 0;
	Attribute fieldAttr;
	for (; conditionIndex < recordDescriptor.size(); conditionIndex++) {
		fieldAttr = recordDescriptor[conditionIndex];
		if (conditionAttribute.compare(fieldAttr.name) == 0) {
			break;
		}
	}
	rbfm_ScanIterator.conditionAttrIndex = conditionIndex;
	rbfm_ScanIterator.op = compOp;

	// initialize predicate value
	if (value == NULL) {
		rbfm_ScanIterator.value = NULL;
	} else {
		unsigned dataLength = fieldAttr.type == TypeVarChar ? strlen((char*)value) + 1 : sizeof(int);
		rbfm_ScanIterator.value = malloc(dataLength);
		memcpy(rbfm_ScanIterator.value, value, dataLength);
	}

	// store record descriptor
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	// store projected attributes' index
	for (int i = 0; i < attributeNames.size(); i++) {
		string name = attributeNames[i];
		for (int j = 0; j < recordDescriptor.size(); j++) {
			if (recordDescriptor[j].name.compare(name) == 0) {
				rbfm_ScanIterator.projectedDescriptor.push_back(j);
			}
		}

	}

    return 0;
}



RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 

	if (!opened) {
		return -1;
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	
	// case 1: pageNum >= totalPageNum
	unsigned totalPageNum = fileHandle.getNumberOfPages();
	if (currentRid.pageNum >= totalPageNum) {
		return RBFM_EOF;
	}

	Page *page = new Page;
	fileHandle.readPage(currentRid.pageNum, page);
	short slotCount = page->header.slotCount;

	// case 2: last record visited on this page, need to scan next page
	if (currentRid.slotNum == slotCount - 1) {
		currentRid.pageNum = currentRid.pageNum + 1;
		currentRid.slotNum = -1;
		free(page);
		return getNextRecord(rid, data);
	}

	// case 3: current record is not the last record on its page, we continue on this page
	Slot nextSlot = {};
	currentRid.slotNum += 1;
	rbfm->readSlotFromPage(page, currentRid.slotNum, nextSlot);

	// case 3.1: the next slot points to a deleted or redirected record, skip it
	if (nextSlot.offset == -1 || nextSlot.length == -1) {
		free(page);
		return getNextRecord(rid, data);
	}

	// case 3.2: the next record is on this page

	// read out the record
	void *nextInnerRecord = malloc(PAGE_SIZE);
	rbfm->readRecordFromPage(page, nextSlot.offset, nextSlot.length, nextInnerRecord);

	// read out the attribute data
	Attribute conditionAttr = recordDescriptor[conditionAttrIndex];
	void *attributeData = malloc(PAGE_SIZE);
	rbfm->readAttribute(fileHandle, recordDescriptor, currentRid, conditionAttr.name, attributeData);

	// case 3.2.1: condition not met, skip this record
	if (!opCompare(attributeData, value, op, conditionAttr.type)) {
		free(page);
		free(nextInnerRecord);
		free(attributeData);
		return getNextRecord(rid, data);
	}

	// case 3.2.2: condition met, compose a tuple of this record into data
	short tupleSize = 0;
	rbfm->composeApiTuple(recordDescriptor, projectedDescriptor, nextInnerRecord, data, tupleSize);

	rid.pageNum = currentRid.pageNum;
	rid.slotNum = currentRid.slotNum;
	free(page);
	free(nextInnerRecord);
	free(attributeData);
	return 0;
};




RC RBFM_ScanIterator::close() { 
	opened = false;
	op = NO_OP;
	conditionAttrIndex = -1;
	free(value);
	recordDescriptor.clear();
	projectedDescriptor.clear();
	return 0; 
};




// note: if TypeVarChar, ref1 and ref2 are already formatted to string
bool RBFM_ScanIterator::opCompare(void* ref1, void* ref2, CompOp op, AttrType type) {
    if (op == NO_OP) {
    	return true;
    }
    if (!ref1 || !ref2) {
    	return false;
    }
    if (type == TypeVarChar) {
		string str1((char*)ref1);
		string str2((char*)ref2);
		int res = str1.compare(str2);
		switch (op) {
			case EQ_OP: return res == 0;
        	case LT_OP: return res < 0;
        	case LE_OP: return res <= 0;
        	case GT_OP: return res > 0;
        	case GE_OP: return res >= 0;
        	case NE_OP: return res != 0; 
        	default: return false;
		}
    }
    else if (type == TypeInt) {
        int int1 = *(int*)ref1;
        int int2 = *(int*)ref2;
        int res = int1 - int2;
        switch (op) {
			case EQ_OP: return res == 0;
        	case LT_OP: return res < 0;
        	case LE_OP: return res <= 0;
        	case GT_OP: return res > 0;
        	case GE_OP: return res >= 0;
        	case NE_OP: return res != 0; 
        	default: return false;
		}

    }
    else {
        float f1 = *(float*)ref1;
        float f2 = *(float*)ref2;
        float res = f1 - f2;

        switch (op) {
			case EQ_OP: return res == 0.0;
        	case LT_OP: return res < 0.0;
        	case LE_OP: return res <= 0.0;
        	case GT_OP: return res > 0.0;
        	case GE_OP: return res >= 0.0;
        	case NE_OP: return res != 0.0; 
        	default: return false;
		}
    }
    return false;
}