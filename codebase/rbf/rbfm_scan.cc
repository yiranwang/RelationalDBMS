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
      	RBFM_ScanIterator &rbfm_ScanIterator) 
{

	if (fileHandle.fd < 0) {
        return -1;
    }
	rbfm_ScanIterator.opened = true;
	rbfm_ScanIterator.fileHandle = fileHandle;

	// find condition attribute by name
	int conditionIndex = 0;
	Attribute fieldAttr;
	for (; conditionIndex < recordDescriptor.size(); conditionIndex++) {
		fieldAttr = recordDescriptor[conditionIndex];
		if (conditionAttribute.compare(fieldAttr.name) == 0) {
			//printf("conditionAttribute is: %s, index: %d\n", fieldAttr.name.c_str(), conditionIndex);
			break;
		}
	}

	rbfm_ScanIterator.conditionAttrIndex = conditionIndex;
	rbfm_ScanIterator.op = compOp;

	// initialize predicate value
	// TypeVarChar case: value is in length|data format
	if (value == NULL) {
		rbfm_ScanIterator.value = NULL;
	} else {
		unsigned dataLength = sizeof(int);

		// build a string in case of TypeVarChar
		if (fieldAttr.type == TypeVarChar) {
			dataLength = *(int*)value;
			rbfm_ScanIterator.value = malloc(dataLength + 1);
			memcpy(rbfm_ScanIterator.value, (char*)value + sizeof(int), dataLength);
			*((char*)(rbfm_ScanIterator.value) + dataLength)= '\0';
		}
		else {
			rbfm_ScanIterator.value = malloc(dataLength);
			memcpy(rbfm_ScanIterator.value, value, dataLength);
		}	
		
		//printf("copied data of length %d, which is: %s\n", dataLength, (char*)(rbfm_ScanIterator.value));
	}

	// store record descriptor and projected attributes descriptor and index
	rbfm_ScanIterator.recordDescriptor = recordDescriptor;

	for (int i = 0; i < attributeNames.size(); i++) {
		for (int j = 0; j < recordDescriptor.size(); j++) {
			if (recordDescriptor[j].name.compare(attributeNames[i]) == 0) {
				rbfm_ScanIterator.projectedDescriptor.push_back(recordDescriptor[j]);
				rbfm_ScanIterator.projectedDescriptorIndex.push_back(j);
			}
		}
	}
    return 0;
}



RC RBFM_ScanIterator::close() { 
	tableName = "";
	nextRid.pageNum = 0;
    nextRid.slotNum = 0;
	opened = false;
	op = NO_OP;
	conditionAttrIndex = -1;
	if (!value) {
		free(value);
		value = NULL;
	}
	recordDescriptor.clear();
	projectedDescriptor.clear();
	projectedDescriptorIndex.clear();
	return 0; 
};



RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 

	// conditionAttribute not exist and not NO_OP, just return -1
	if (conditionAttrIndex == recordDescriptor.size() && op != NO_OP) {
		return RBFM_EOF;
	}

	if (DEBUG) printf("Scanning ...next RID = (%d, %d)\n", nextRid.pageNum, nextRid.slotNum);

	if (!opened) {
		if(DEBUG) printf("Sorry, RBFM_ScanIterator is not opened!\n");
		return -1;
	}


	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

	// case 1: pageNum >= totalPageNum
	unsigned totalPageNum = fileHandle.getNumberOfPages();
	if (nextRid.pageNum >= totalPageNum) {
		if(DEBUG) printf("RBFM_EOF reached!\n");
		return RBFM_EOF;
	}

	// read out the page
	Page *page = new Page;
	if (fileHandle.readPage(nextRid.pageNum, page) < 0) {
		if (DEBUG) printf("fileHandle.readPage(%d) < 0!\n", nextRid.pageNum);
		delete page;
		return -1;
	}

	short slotCount = page->header.slotCount;

	// case 2: invalid slotNum
	if (nextRid.slotNum > (short)slotCount) {
		if (DEBUG) printf("nextRid.slotNum:%d >= slotCount:%d\nSo record to be read does not exist\n", nextRid.slotNum, slotCount);
		delete page;
		return -1;
	}

	// case 3: previous record visited is the last one on this page, need to scan next page
	if (nextRid.slotNum == (unsigned)slotCount) {
		if (DEBUG) printf("previous record visited is the last one on this page, go to the 1st record on the next page\n");
		nextRid.pageNum += 1;
		nextRid.slotNum = 0;
		delete page;
		return getNextRecord(rid, data);
	}


	// case 4: previous record is not the last record on this page, we continue on this page
	Slot targetSlot = {};

	// read out the slot
	rbfm->readSlotFromPage(page, nextRid.slotNum, targetSlot);
	//printf("slot read out: offset = %d, length = %d!\n", targetSlot.offset, targetSlot.length);

	// case 4.1: the next slot points to a deleted record or it's redirected from somewhere else, skip it
	if (targetSlot.offset == -1 || targetSlot.isRedirected != 0) {
		if (DEBUG) printf("the slot points to a deleted or redirected record, skip it\n");
		delete page;
		nextRid.slotNum += 1;						// go for next slot
		return getNextRecord(rid, data);
	}


	// ****************************** general case ******************************

	// case 4.2: the target record is on this page

	// read out the inner record
	// printf("allocating memory for tmpInnerRecord ...\n");
	void *tmpInnerRecord = malloc(PAGE_SIZE);

	//printf("tmpInnerRecord malloc(PAGE_SIZE) done\n");
	//memset(tmpInnerRecord, 0, PAGE_SIZE);
	rbfm->readRecordFromPage(page, targetSlot.offset, targetSlot.length, tmpInnerRecord);

	//printf("inner record read out!\n");

	// comparison is needed only if op != NO_OP
	// conditionAttriIndex is a valid index now, otherwise -1 is returned at the beginning of this function
	if (op != NO_OP) {
		// read out the attribute value to be compared
		Attribute conditionAttr = recordDescriptor[conditionAttrIndex];				
		void *attributeData = malloc(PAGE_SIZE);
		rbfm->readAttributeFromInnerRecord(recordDescriptor, tmpInnerRecord, conditionAttrIndex, attributeData);

		//printf("attribute read out!\n");

		// attributeData's 1st byte is nullIndicator: 10000000 means it's NULL
		// case 4.2.1: this attribute is NULL or condition not met, skip this record
		if (*(unsigned char*)attributeData != 0 || !opCompare((char*)attributeData + 1, value, op, conditionAttr.type)) {
			if (DEBUG) printf("this attribute is NULL or condition not met, skip this record\n");
			delete page;
			free(tmpInnerRecord);
			free(attributeData);
			nextRid.slotNum += 1;						// go for next slot
			return getNextRecord(rid, data);
		}
		free(attributeData);
	}


	//printf("[Condition met!]\n");
	// case 4.2.2: condition met, compose a tuple of this record into data
	short tupleSize = 0;
	rbfm->composeApiTuple(recordDescriptor, projectedDescriptorIndex, tmpInnerRecord, data, tupleSize);
	
	//printf("\n[Condition Met] the tuple is from this record at RID(%d, %d):\n", nextRid.pageNum, nextRid.slotNum);
	//rbfm->printInnerRecord(recordDescriptor, tmpInnerRecord);
	//printf("\n");
	

	//printf("Next qualified tuple is:\n");
	//rbfm->printRecord(projectedDescriptor, data);

	rid.pageNum = nextRid.pageNum;
	rid.slotNum = nextRid.slotNum;
	delete page;
	free(tmpInnerRecord);

	//printf("free tmpInnerRecord and attributeData done\n");
	nextRid.slotNum += 1;						// go for next slot

	//printf("------------------------------------------------------\n");
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
    	//printf("comparing %s and %s\n", (char*)ref1, (char*)ref2);
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
        //printf("comparing %d and %d\n", int1, int2);
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
        //printf("comparing %f and %f\n", f1, f2);
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