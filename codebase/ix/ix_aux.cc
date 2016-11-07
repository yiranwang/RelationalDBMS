
#include <cstdlib>
#include "ix.h"
#include<iostream>

int IndexManager::key_length(const AttrType attrType, const void* key){
    if(attrType == TypeVarChar){
        int len = *(int*)key;
        return sizeof(int) + len;
    }
    else{
        return sizeof(int);
    }
}

int IndexManager::compareKey(const void *key1, const void *key2, const AttrType attrType) {
    if (attrType == TypeInt) {
        int k1 = *(int*)key1;
        int k2 = *(int*)key2;
        return *(int*)key1 - *(int*)key2;
    }
    else if (attrType == TypeReal) {
        if (memcmp(key1, key2, sizeof(float)) == 0) {
            return 0;
        }
        else {
            return *(float*)key1 - *(float*)key2 > 0 ? 1 : -1;
        }
    } else {
        size_t length1 = (size_t)*(int*)key1;
        size_t length2 = (size_t)*(int*)key2;

        char *str1 = (char*)malloc(length1 + 1);
        char *str2 = (char*)malloc(length2 + 1);

        memcpy(str1, (char*)key1 + sizeof(int), length1);
        str1[length1] = '\0';
        memcpy(str2, (char*)key2 + sizeof(int), length2);
        str2[length2] = '\0';

        int res = strcmp(str1, str2);
        free(str1);
        free(str2);
        return res;
    }
}



IXPage* IndexManager::initializeIXPage(unsigned pageNum, char pageType, AttrType attrType) {
    IXPage *newPage = new IXPage;

    newPage->header.pageNum = pageNum;

    //initialize parent pointer
    if (pageType == DIR_PAGE_TYPE) {
        newPage->header.parent = 0;

        newPage->header.leftmostPtr = 0;
        newPage->header.prevPageNum = 0;
        newPage->header.nextPageNum = 0;
        newPage->header.isRoot = false;

    }else if (pageType == INDEX_PAGE_TYPE) {
        newPage->header.parent = 1;
        newPage->header.leftmostPtr = 2;

        newPage->header.prevPageNum = 0;
        newPage->header.nextPageNum = 0;
        newPage->header.isRoot = true;

    }else if (pageType == LEAF_PAGE_TYPE) {
        newPage->header.parent = 1;
        newPage->header.leftmostPtr = 0;
        newPage->header.prevPageNum = 2;
        newPage->header.nextPageNum = 2;
        newPage->header.isRoot = false;
    }


    newPage->header.freeSpaceSize = PAGE_SIZE - sizeof(IXPageHeader);
    newPage->header.freeSpaceOffset = sizeof(IXPageHeader);

    //newPage->header.pageType = 0;
    newPage->header.pageType = pageType;

    newPage->header.attrType = attrType;

    newPage->header.entryCount = 0;

    return newPage;
}


RC IndexManager::initializeIndex(IXFileHandle &ixfileHandle, const AttrType attrType) {

    // insert directory page, set attribute type
    IXPage *dirPage = initializeIXPage(0, DIR_PAGE_TYPE, attrType);
    dirPage->header.leftmostPtr = 1;                     // set next page as root page
    ixfileHandle.appendPage(dirPage);
    delete(dirPage);

    // insert root index page
    IXPage *rootPage = initializeIXPage(1, INDEX_PAGE_TYPE, attrType);

    // initialize leaf page
    IXPage *leafPage = initializeIXPage(2, LEAF_PAGE_TYPE, attrType);

    //insertIndexEntryToIndexPage();
    ixfileHandle.appendPage(rootPage);
    ixfileHandle.appendPage(leafPage);
    delete(rootPage);
    delete(leafPage);

    return 0;
}

void IndexManager::insertEntryToEmptyRoot(IXFileHandle &ixfileHandle, IXPage *rootPage, const void *key, const RID &rid) {

    IXPage *prevLeafPage = new IXPage;
    ixfileHandle.readPage(rootPage->header.leftmostPtr, prevLeafPage);

    IXPage *leafPage = initializeIXPage(ixfileHandle.fileHandle.getNumberOfPages(), LEAF_PAGE_TYPE,
                                        rootPage->header.attrType);

    int keyLength = key_length(rootPage->header.attrType, key);
    memcpy((char*)leafPage + sizeof(IXPageHeader), (char*)key, keyLength);
    *(RID*)((char*)leafPage + sizeof(IXPageHeader) + keyLength) = rid;


    leafPage->header.leftmostPtr = 0;
    leafPage->header.prevPageNum = prevLeafPage->header.pageNum;
    leafPage->header.nextPageNum = leafPage->header.pageNum;
    leafPage->header.freeSpaceOffset = sizeof(IXPageHeader) + keyLength + sizeof(RID);
    leafPage->header.freeSpaceSize = PAGE_SIZE - leafPage->header.freeSpaceOffset;
    leafPage->header.attrType = rootPage->header.attrType;
    leafPage->header.entryCount = 1;
    leafPage->header.parent = rootPage->header.pageNum;
    leafPage->header.lastEntryOffset = sizeof(IXPageHeader);
    leafPage->header.isRoot = false;

    ixfileHandle.appendPage(leafPage);

    prevLeafPage->header.nextPageNum = leafPage->header.pageNum;

    // insert new entry into empty root
    memcpy((char*)rootPage->data, (char*)key, keyLength);
    *(int*)((char*)rootPage->data + keyLength) = leafPage->header.pageNum;
    rootPage->header.entryCount++;
    rootPage->header.freeSpaceOffset += (keyLength + sizeof(int));
    rootPage->header.freeSpaceSize -= (keyLength + sizeof(int));
    rootPage->header.lastEntryOffset = sizeof(IXPageHeader);

    //ixfileHandle.writePage(leafPage->header.pageNum, leafPage);
    ixfileHandle.writePage(prevLeafPage->header.pageNum, prevLeafPage);
    ixfileHandle.writePage(rootPage->header.pageNum, rootPage);

    delete(leafPage);
    delete(prevLeafPage);
}


void IndexManager::insertTree(IXFileHandle &ixfileHandle, IXPage *page, const void *key, const RID &rid, void* &newChildEntry) {

    char pageType = page->header.pageType;
    // case 1: the page to be inserted is leaf page
    if (pageType == LEAF_PAGE_TYPE) {

        int keyLength = key_length(page->header.attrType, key);
        int insertSize = keyLength + sizeof(RID);
        bool needSplit = page->header.freeSpaceSize - insertSize < 0;

        int countNode = 0;
        // find insert offset in page->data(!!!!! don't include IXPageHeader size!!!!!!!!!!)
        int insertOffset = findInsertOffset(page, key, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset - sizeof(IXPageHeader);

        // case 1.1: enough space in leaf page, so no need to split leaf page
        if (!needSplit) {
            // copy the rest leaf nodes to restNodes
            void *restNodes = malloc((size_t)restSize);

            int offset = insertOffset + sizeof(IXPageHeader);
            memcpy(restNodes, (char*)page + offset, (size_t)restSize);


            memcpy((char*)page + offset, key, (size_t)keyLength);

            offset += keyLength;

            *(RID*)((char*)page + offset) = rid;
            offset += sizeof(RID);

            memcpy((char*)page + offset, (char*)restNodes, (size_t)restSize);

            //modify freespace and entryCount;
            page->header.entryCount++;
            page->header.freeSpaceSize -= (keyLength + sizeof(RID));
            page->header.freeSpaceOffset += keyLength + sizeof(RID);

            int prevLastEntryLength = key_length(page->header.attrType, (char*)page + page->header.lastEntryOffset);
            page->header.lastEntryOffset += prevLastEntryLength + sizeof(RID);

            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);

            free(restNodes);
        }
        // case 1.2: not enough space in leaf page, need to split this leaf page
        else {
            // split leaf page
            // malloc new 2*pagesize space to copy original nodes + new node, no page header included

            // original page: [header, entry0, entry1, ..., entryM, \0, ..., \0]

            //insertOffset doesn't include IXPageHeader size!!!!!!!!!!!
            void *doubleSpace = malloc(PAGE_SIZE * 2);
            int offsetInDoubleSpace = 0;
            int offsetInOldPage = sizeof(IXPageHeader);

            //copy first part
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)page + offsetInOldPage, insertOffset);
            offsetInOldPage += insertOffset;
            offsetInDoubleSpace += insertOffset;

            //copy inserted key
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)key, keyLength);
            offsetInDoubleSpace += keyLength;
            *(RID*)((char*)doubleSpace + offsetInDoubleSpace) = rid;
            offsetInDoubleSpace += sizeof(RID);

            //copy rest part
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)page + offsetInOldPage, restSize);

            // now  doubleSpace: [entry0, entry1, ..., entryInsert, ..., entryM, ..., \0, \0, \0,]


            int allEntryCount = page->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            //calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            int firstHalfOffset = 0;                // offset of the second half's starting address (inside page->data)
            int prevFirstHalfOffset = 0;            // offset of the last entry in the first half (inside page->data)
            for (int i = 0; i < halfEntryCount; i++) {
                prevFirstHalfOffset = firstHalfOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                firstHalfOffset += curKeyLength + sizeof(RID);
            }



            //compose newChildEntry that will be returned
            // (COPY UP) newChildEntry : key + PID
            int returnedKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
            void *willReturn = malloc(returnedKeyLength + sizeof(int));
            memcpy((char*)willReturn, (char*)doubleSpace + firstHalfOffset, returnedKeyLength);
            *(int*)((char*)willReturn + returnedKeyLength) = ixfileHandle.fileHandle.getNumberOfPages();
            if (newChildEntry) {
                free(newChildEntry);
            }
            newChildEntry = willReturn;

            //calculate offset of the second half
            int totalOffset = firstHalfOffset;
            int prevTotalOffset = firstHalfOffset;
            for (int i = 0; i < allEntryCount - halfEntryCount; i++) {
                prevTotalOffset = totalOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
                totalOffset += curKeyLength + sizeof(RID);
            }

            // copy the firstHalf from doublespace to page
            memcpy(page->data, (char*)doubleSpace, firstHalfOffset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy(newPage->data, (char*)doubleSpace + firstHalfOffset, totalOffset - firstHalfOffset);

            //modify header of new page--------------------
            newPage->header.entryCount = allEntryCount - halfEntryCount;
            newPage->header.attrType = page->header.attrType;
            newPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - firstHalfOffset;
            newPage->header.freeSpaceSize = PAGE_SIZE - (totalOffset - firstHalfOffset) - sizeof(IXPageHeader);
            newPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();
            newPage->header.leftmostPtr = 0;
            newPage->header.prevPageNum = page->header.pageNum;

            // current page is the last page
            if (page->header.nextPageNum == page->header.pageNum) {
                newPage->header.nextPageNum = newPage->header.pageNum;
            }else {
                //current page is not the last page
                newPage->header.nextPageNum = page->header.nextPageNum;
            }
            newPage->header.parent = page->header.parent;
            newPage->header.lastEntryOffset = prevTotalOffset - firstHalfOffset + sizeof(IXPageHeader);
            newPage->header.pageType = LEAF_PAGE_TYPE;
            newPage->header.isRoot = false;

            ixfileHandle.appendPage(newPage);


            //modify header of original page ------------------------
            page->header.entryCount = halfEntryCount;
            page->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
            page->header.freeSpaceSize = PAGE_SIZE - firstHalfOffset - sizeof(IXPageHeader);;
            page->header.nextPageNum = newPage->header.pageNum;
            page->header.lastEntryOffset = prevFirstHalfOffset + sizeof(IXPageHeader);

            ixfileHandle.writePage(newPage->header.pageNum, newPage);
            ixfileHandle.writePage(page->header.pageNum, page);

            delete(newPage);
            free(doubleSpace);
        }
    }
    // case 2: the page is not leaf page
    else {
        IXPage *nextPage = findNextPage(ixfileHandle, page, key);

        insertTree(ixfileHandle, nextPage, key, rid, newChildEntry); //recursively insert entry in the leaf page

        // case 2.1: usual case, child is not split, done
        if (newChildEntry == NULL) {
            delete(nextPage);
            return;
        }

        // case 2.2: child is split, must insert newchildentry
        int keyLength = key_length(page->header.attrType, key);
        int insertSize = keyLength + sizeof(unsigned);
        bool needSplit = page->header.freeSpaceSize - insertSize < 0;

        int countNode = 0;
        // count the first half entry

        // !!!!!insertOffset don't include IXPageHeader size!!!!!!!!!!!!!!
        int insertOffset = findInsertOffset(page, key, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset - sizeof(IXPageHeader);

        // case 2.2.1: if no need to split this index page
        if (!needSplit) {
            int offset = sizeof(IXPageHeader) + insertOffset;

            // copy rest part out to restNodes
            void *restNodes = malloc(restSize);
            memcpy((char*)restNodes, (char*)page + offset + insertOffset, restSize);

            // insert newChildEntry to page
            // insert newCHildEntry's key
            int newChildEntryKeyLength = key_length(page->header.attrType, newChildEntry);
            memcpy((char*)page + offset, (char*)newChildEntry, newChildEntryKeyLength);
            offset += newChildEntryKeyLength;

            // insert newChildEntry's PID
            *(unsigned*)((char*)page + offset) = *(unsigned*)((char*)newChildEntry + newChildEntryKeyLength);
            offset += sizeof(unsigned);

            // copy rest nodes from restNodes back to page
            memcpy((char*)page + offset, (char*)restNodes, restSize);

            // adjust freespace, entrycount, lastEntryOffset in page
            page->header.entryCount += 1;
            page->header.freeSpaceSize -= newChildEntryKeyLength + sizeof(unsigned);
            page->header.freeSpaceOffset += newChildEntryKeyLength + sizeof(unsigned);

            int prevLastEntryLength = key_length(page->header.attrType, (char*)page + page->header.lastEntryOffset);
            page->header.lastEntryOffset += prevLastEntryLength + sizeof(unsigned);

            free(newChildEntry);
            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);

            free(restNodes);

        }
        // case 2.2.2: need to split this index page
        else {

            int allEntryCount = page->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            //malloc new space to copy original page + inserted key
            void *doubleSpace = malloc(PAGE_SIZE * 2);

            int offsetInOldPage = sizeof(IXPageHeader);
            int offsetInDoubleSpace = 0;  // offset in doubleSpace
            // copy first half
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)page + offsetInOldPage, insertOffset);
            offsetInOldPage += insertOffset;
            offsetInDoubleSpace += insertOffset;

            // insert newChildEntry into double space
            int newChildEntryKeyLength = key_length(page->header.attrType, newChildEntry);
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)newChildEntry, newChildEntryKeyLength + sizeof(unsigned));
            offsetInDoubleSpace += newChildEntryKeyLength + sizeof(unsigned);

            //copy the other half
            memcpy((char*)doubleSpace + offsetInDoubleSpace, (char*)page + offsetInOldPage, restSize);


            // locate the first half; calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            int firstHalfOffset = 0;
            int prevFirstHalfOffset = 0;
            for (int i = 0; i < halfEntryCount; i++) {
                prevFirstHalfOffset = firstHalfOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                firstHalfOffset += curKeyLength + sizeof(int);
            }


            // locate the offsets of the end of other half and the last entry
            int totalOffset = firstHalfOffset;                  // end
            int prevTotalOffset = firstHalfOffset;              // last entry

            // compose newChildEntry that will be returned (pushed up)
            // newChildEntry : key + PID
            int returnedKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
            unsigned returnedPID = *(unsigned*)((char*)doubleSpace + totalOffset + returnedKeyLength);

            void *willReturn = malloc(returnedKeyLength + sizeof(unsigned));
            memcpy((char*)willReturn, (char*)doubleSpace + totalOffset, returnedKeyLength + sizeof(unsigned));
            if (newChildEntry) {
                free(newChildEntry);
            }
            newChildEntry = willReturn;

            totalOffset += returnedKeyLength + sizeof(unsigned);
            int secondHalfBeginOffset = totalOffset;

            //calculate offset of the end and the last entry of the other half except the returned entry
            for (int i = 0; i < allEntryCount - halfEntryCount - 1; i++) {
                prevTotalOffset = totalOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
                totalOffset += curKeyLength + sizeof(unsigned);
            }

            // copy the firstHalf from doublespace to page
            memcpy(page->data, (char*)doubleSpace, firstHalfOffset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy(newPage->data, (char*)doubleSpace + secondHalfBeginOffset, totalOffset - secondHalfBeginOffset);

            //modify header of new page--------------------
            newPage->header.entryCount = allEntryCount - halfEntryCount;
            newPage->header.pageType = INDEX_PAGE_TYPE;
            newPage->header.attrType = page->header.attrType;
            newPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - firstHalfOffset;
            newPage->header.freeSpaceSize = PAGE_SIZE - (totalOffset - firstHalfOffset) - sizeof(IXPageHeader);
            newPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();
            newPage->header.leftmostPtr = returnedPID;
            newPage->header.prevPageNum = 0;
            newPage->header.nextPageNum = 0;
            newPage->header.parent = page->header.parent;
            newPage->header.lastEntryOffset = prevTotalOffset - secondHalfBeginOffset + sizeof(IXPageHeader);
            newPage->header.isRoot = false;

            ixfileHandle.appendPage(newPage);


            //modify header of the original page ------------------------
            page->header.entryCount = halfEntryCount;
            page->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
            page->header.freeSpaceSize = PAGE_SIZE - firstHalfOffset - sizeof(IXPageHeader);

            page->header.lastEntryOffset = prevFirstHalfOffset + sizeof(IXPageHeader);

            // if current page is the root page, create a new root index page
            if (page->header.isRoot) {
                IXPage *newRootPage = new IXPage;

                // insert newChildEntry into new root page
                unsigned newChildEntryLength = newChildEntryKeyLength + sizeof(unsigned);
                memcpy(newRootPage->data, (char*)newChildEntry, newChildEntryLength);

                newRootPage->header.leftmostPtr = page->header.pageNum;
                newRootPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();
                newRootPage->header.attrType = page->header.attrType;
                newRootPage->header.entryCount = 1;
                newRootPage->header.freeSpaceOffset = sizeof(IXPageHeader) + newChildEntryLength;
                newRootPage->header.freeSpaceSize = PAGE_SIZE - sizeof(IXPageHeader) - newChildEntryLength;
                newRootPage->header.isRoot = true;
                newRootPage->header.pageType = INDEX_PAGE_TYPE;
                newRootPage->header.prevPageNum = 0;
                newRootPage->header.nextPageNum = 0;
                newRootPage->header.parent = newRootPage->header.pageNum;
                newRootPage->header.lastEntryOffset = sizeof(IXPageHeader);

                ixfileHandle.appendPage(newRootPage);

                page->header.isRoot = false;
                page->header.parent = newRootPage->header.pageNum;

                // adjust rootPage number in dirPage
                IXPage *dirPage = new IXPage;
                ixfileHandle.readPage(0, dirPage);
                dirPage->header.leftmostPtr = newRootPage->header.pageNum;
                ixfileHandle.writePage(0, dirPage);


                printf("************ Splitted a root!\n");

                delete(newRootPage);
                delete(dirPage);
            }

            ixfileHandle.writePage(page->header.pageNum, page);
            ixfileHandle.writePage(newPage->header.pageNum, newPage);

            free(newChildEntry);
            //free(willReturn);
            free(doubleSpace);
            delete(newPage);
        }

        delete(nextPage);
    }

}



// find the next child non leaf page one by one level down
IXPage* IndexManager::findNextPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key) {
    if (page->header.entryCount == 0) {
        return page;
    }

    int nextPageNum = 0;
    IXPage *nextPage = new IXPage;

    // if key < firstKey, skip
    char* firstKey = (char*)page + sizeof(IXPageHeader);

    if (compareKey(key, firstKey, page->header.attrType) < 0) {

        nextPageNum = page->header.leftmostPtr;

        ixfileHandle.readPage(nextPageNum, nextPage);

        return nextPage;
    }

    // if key >= lastKey, skip
    void* lastKey = (char*)page + page->header.lastEntryOffset;

    if (compareKey(key, lastKey, page->header.attrType) >= 0) {

        nextPageNum = *(int*)((char*)page + page->header.freeSpaceOffset - sizeof(int));
        ixfileHandle.readPage(nextPageNum, nextPage);
        return nextPage;
    }

    // if firstKey < key < lastKey
    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount - 1; i++) {
        void* curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        void* nextKey = (char*)page + offset + curKeyLength + sizeof(int);


        if (compareKey(key, curKey, page->header.attrType) >= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {

            nextPageNum = *(int*)(page + offset + curKeyLength);
            ixfileHandle.readPage(nextPageNum, nextPage);
            return nextPage;
        }
        offset += curKeyLength + sizeof(int);
    }

    delete(nextPage);

    return NULL;
}

//find insert offset within a page(!!!!! don't include IXPageHeader size!!!!!!!!!!)
// return the offset inside page->data where the key should be inserted (page->data + offset)
int IndexManager::findInsertOffset(IXPage *page, const void *key, int &countNode) {

    if (page->header.entryCount == 0) {
        return 0;
    }

    int ridOrPidSize = 0;

    if (page->header.pageType == LEAF_PAGE_TYPE) {
        ridOrPidSize = sizeof(RID);
    }else {
        ridOrPidSize = sizeof(unsigned);
    }

    int entryCount = page->header.entryCount;

    void *lastKey = (char*)page + page->header.lastEntryOffset;


    //key >= lastKey, no need of scanning previous n-1 entries in this page
    if (compareKey(key, lastKey, page->header.attrType) >= 0) {
        countNode = entryCount;
        return page->header.freeSpaceOffset - sizeof(IXPageHeader);
    }

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount - 1; i++) {
        void *curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);

        if (compareKey(curKey, key, page->header.attrType) > 0 ) {
            return offset - sizeof(IXPageHeader);
        }
        countNode++;
        offset += curKeyLength + ridOrPidSize;
    }

    return -1;
}


/*void IndexManager::readIndexEntryFromIndexPage(void *entry, void *page, short pos) {
    int keyLength = page->header.attrType == TypeVarChar? sizeof(int);

    short entryOffset = (char*) page + sizeof(IXPageHeader) +
    if () {
        int varCharLen = *(int*)entry;

    }

    memcpy(entry, (char*)page + sizeof(IXPageHeader), sizeof(int) + varCharLen);
}*/


IXPage* IndexManager::findFirstLeafPage(IXFileHandle &ixfileHandle, IXPage *page) {
    int pageType = page->header.pageType;

    // case 1: input page is a leaf page
    if (pageType == 1) {
        if (page->header.entryCount == 0) {
            unsigned nextPageNum = page->header.nextPageNum;
            ixfileHandle.readPage(nextPageNum, page);
            return findFirstLeafPage(ixfileHandle, page);
        }
        return page;
    }

    unsigned nextPageNum = page->header.leftmostPtr;
    ixfileHandle.readPage(nextPageNum, page);
    return findFirstLeafPage(ixfileHandle, page);
}

// search recursively
IXPage * IndexManager::findLeafPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key) {

    int pageType = page->header.pageType;

    // case 1: input page is a leaf page
    if (pageType == 1) {
        return page;
    }

    // case 2: no leaf pages yet, create an empty one and return it
    /*if (page->header.entryCount == 0) {
        IXPage *leafPage = initializeIXPage(2, LEAF_PAGE_TYPE, indexPage->header.attrType);
        insertDataEntryToLeafPage(leafPage, key, rid);
        leafPage->header.entryCount++;
        ixfileHandle.appendPage(leafPage);
        return leafPage;
    }*/

    // case 2: input page is an index page
    // case 2: compare the keys sequentially

    int nextPageNum = 0;
    IXPage *nextPage = new IXPage;

    // if key < firstKey, skip
    void* firstKey = (char*)page + sizeof(IXPageHeader);
    if (compareKey(key, firstKey, page->header.attrType) < 0) {

        nextPageNum = page->header.leftmostPtr;

        ixfileHandle.readPage(nextPageNum, nextPage);

        return findLeafPage(ixfileHandle, nextPage, key);
    }

    // if key >= lastKey, skip
    void* lastKey = page + page->header.lastEntryOffset;
    if (compareKey(key, lastKey, page->header.attrType) >= 0) {

        nextPageNum = *(int*)((char*)page + page->header.freeSpaceOffset - sizeof(int));
        ixfileHandle.readPage(nextPageNum, nextPage);
        return findLeafPage(ixfileHandle, nextPage, key);
    }

    // if firstKey < key < lastKey
    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount - 1; i++) {
        void* curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        void* nextKey = (char*)page + offset + curKeyLength + sizeof(int);

        if (compareKey(key, curKey, page->header.attrType) >= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            nextPageNum = *(int*)(page + offset + curKeyLength);
            ixfileHandle.readPage(nextPageNum, nextPage);
            return findLeafPage(ixfileHandle, nextPage, key);
        }
        offset += curKeyLength + sizeof(int);
    }

    delete(nextPage);

    return NULL;

}

void IndexManager::DFSPrintBTree(int pageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const{
    IXPage *page = new IXPage;
    ixfileHandle.readPage(pageNum, page);
    int entryCount = page->header.entryCount;

    if (entryCount == 0) {
        return;
    }

    if (attribute.type == TypeInt) {

        // index page
        if (page->header.pageType == INDEX_PAGE_TYPE) {
            vector<int> keys;
            vector<int> pids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                int key = *(int*)((char*)page + offset);
                offset += sizeof(int);
                keys.push_back(key);
                int pid = *(int*)((char*)page + offset);
                offset += sizeof(int);
                pids.push_back(pid);
            }

            cout<< "{\"keys\": [";
            for (int i = 0; i < keys.size(); i++) {
                cout << "\"" << keys[i] << "\"";
                if (i != keys.size() - 1) {
                    cout << ",";
                }
            }
            cout << "]," << endl;
            cout << "\"children\": [" << endl;

            for (int i = 0; i < pids.size(); i++) {
                if (i != 0) {
                    cout << "," << endl;
                }
                DFSPrintBTree(pids[i], ixfileHandle, attribute);
                cout << "]}";
            }

            // is leaf page
        }else {
            vector<int> keys;
            vector<RID> rids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                int key = *(int*)((char*)page + offset);
                offset += sizeof(int);
                keys.push_back(key);
                RID rid = *(RID*)((char*)page + offset);
                offset += sizeof(RID);
                rids.push_back(rid);
            }
            cout << "{\"keys\": [";

            int i = 0;
            for (i = 0; i < keys.size(); i++) {
                if (i == 0) {
                    cout << "\"" << keys[i] << ":[";

                }else if (keys[i] != keys[i - 1]) {
                    cout << "]\",";
                    cout << "\"" << keys[i] << ":[";

                }else{
                    cout << ",";
                }

                cout << "(" << rids[i].pageNum << "," << rids[i].slotNum << ")";
            }
            cout << "]\"";

            if (i == keys.size() && page->header.nextPageNum == page->header.pageNum) {
                cout << "]}" << endl;
            }

        }

    }else if (attribute.type == TypeReal) {

        // index page
        if (page->header.pageType == INDEX_PAGE_TYPE) {
            vector<float> keys;
            vector<int> pids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                float key = *(float*)((char*)page + offset);
                offset += sizeof(float);
                keys.push_back(key);
                int pid = *(int*)((char*)page + offset);
                offset += sizeof(int);
                pids.push_back(pid);
            }

            cout << "{\"keys\" :[";
            for (int i = 0; i < keys.size(); i++) {
                cout << "\"" << keys[i] << "\"";
                if (i != keys.size() - 1) {
                    cout << ",";
                }
            }
            cout << "]," << endl;
            cout << "\"children\": [" << endl;

            for (int i = 0; i < pids.size(); i++) {
                if (i != 0) {
                    cout<<","<<endl;
                }
                DFSPrintBTree(pids[i], ixfileHandle, attribute);
                cout<<"]}";
            }

            // is leaf page
        }else {
            vector<float> keys;
            vector<RID> rids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                float key = *(float*)((char*)page + offset);
                offset += sizeof(float);
                keys.push_back(key);
                RID rid = *(RID*)((char*)page + offset);
                offset += sizeof(RID);
                rids.push_back(rid);
            }
            cout<< "{\"keys\" :[";

            int i = 0;
            for (i = 0; i < keys.size(); i++) {
                if (i == 0) {
                    cout << "\"" << keys[i] << ":[";

                }else if (keys[i] != keys[i - 1]) {
                    cout <<"]\",";
                    cout << "\"" << keys[i] << ":[";

                }else{
                    cout<<",";
                }

                cout << "(" << rids[i].pageNum << "," << rids[i].slotNum <<")";
            }
            cout << "]\"";

            if (i == keys.size() && page->header.nextPageNum == page->header.pageNum) {
                cout << "]}" << endl;
            }

        }

    }else if (attribute.type == TypeVarChar) {

        char* key = (char*)malloc(PAGE_SIZE);
        int keyLength = 0;

        // index page
        if (page->header.pageType == INDEX_PAGE_TYPE) {
            vector<string> keys;
            vector<int> pids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                keyLength = *(int*)((char*)page + offset);
                offset += sizeof(int);

                memcpy(key, (char*)page + offset, keyLength);
                string s(key);
                keys.push_back(s);
                offset += keyLength;

                int pid = *(int*)((char*)page + offset);
                offset += sizeof(int);
                pids.push_back(pid);
            }

            cout<< "{\"keys\" :[";
            for (int i = 0; i < keys.size(); i++) {
                printf("\"%s\"", keys[i].c_str());
                if (i != keys.size() - 1) {
                    cout<<",";
                }
            }
            cout<<"],"<<endl;
            cout<<"\"children\" :["<<endl;

            for (int i = 0; i < pids.size(); i++) {
                if (i != 0) {
                    cout<<","<<endl;
                }
                DFSPrintBTree(pids[i], ixfileHandle, attribute);
                cout<<"]}";
            }

            // is leaf page
        }else {
            vector<string> keys;
            vector<RID> rids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                keyLength = *(int *) ((char *) page + offset);
                offset += sizeof(int);

                memcpy(key, (char *) page + offset, keyLength);
                string s(key);
                keys.push_back(s);
                offset += keyLength;

                RID rid = *(RID *) ((char *) page + offset);
                offset += sizeof(RID);
                rids.push_back(rid);
            }

            cout<< "{\"keys\" :[";
            int i = 0;
            for (i = 0; i < keys.size(); i++) {
                if (i == 0) {
                    cout << "\""<<keys[i]<<":[";

                }else if (strcmp(keys[i].c_str(), keys[i - 1].c_str()) != 0) {
                    cout <<"]\",";
                    cout << "\""<<keys[i]<<":[";

                }else{
                    cout<<",";
                }

                cout << "(" << rids[i].pageNum << "," << rids[i].slotNum << ")";
            }
            cout << "]\"";

            if (i == keys.size() && page->header.nextPageNum == page->header.pageNum) {
                cout<<"]}" << endl;
            }

        }
        free(key);

    }
    delete(page);
}



// data entry on leaf page: <key, rid>
/*void IndexManager::insertDataEntryToLeafPage(IXPage *leafPage, const void *key, const RID &rid) {
    // find the key length
    int keyLen = leafPage->header.attrType == TypeVarChar ? *(int*)key + sizeof(int) : sizeof(init);

    // compose the data entry
    int entryLen = keyLen + sizeof(RID);
    char *dataEntry = (char*)malloc(entryLen);
    memcpy(dataEntry, key, keyLen);
    memcpy(dataEntry + keyLen, &rid, sizeof(RID));

    // write data entry
    leafPage->header.lastEntryOffset = leafPage->header.freeSpaceOffset;
    memcpy((char*)leafPage + leafPage->header.freeSpaceOffset, entryLen);
    leafPage->header.freeSpaceOffset += entryLen;
    leafPage->header.freeSpaceSize -= entryLen;

}*/






