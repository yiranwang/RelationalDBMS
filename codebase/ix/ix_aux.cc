
#include <cstdlib>
#include "ix.h"
#include<iostream>

int IndexManager::key_length(const AttrType attrType, const void* key){
    if(attrType == TypeVarChar){
        unsigned len = *(unsigned*)key;
        return 4 + len;
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
        return *(float*)key1 - *(float*)key2;
    } else {
        int length1 = *(int*)key1;
        int length2 = *(int*)key2;

        char *str1 = (char*)malloc(length1 + 1);
        char *str2 = (char*)malloc(length2 + 1);

        memcpy(str1, (char*)key1 + length1, length1);
        str1[length1] = '\0';
        memcpy(str2, (char*)key2 + length2, length2);
        str2[length2] = '\0';

        return strcmp(str1, str2);
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
    memcpy((char*)leafPage->data, (char*)key, keyLength);
    *(RID*)((char*)leafPage->data + keyLength) = rid;


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


void IndexManager::insertTree(IXFileHandle &ixfileHandle, IXPage *page, const void *key, const RID &rid,
                              void* &newChildEntry) {

    if (DEBUG) {
        if (page->header.entryCount >= 169) {

        }
    }

    int pageType = page->header.pageType;
    // if the page to insert is leaf page
    if (pageType == LEAF_PAGE_TYPE) {

        int keyLength = key_length(page->header.attrType, key);
        int insertSize = keyLength + sizeof(RID);
        bool needSplit = false;

        if (page->header.freeSpaceSize - insertSize < 0) {
            needSplit = true;
        }

        int countNode = 0;
        // find insert offset in page
        int insertOffset = findInsertOffset(page, key, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset;

        // don't split leaf page
        if (!needSplit) {
            // copy the rest leaf nodes to restNodes
            void *restNodes = malloc(restSize);
            memcpy((char*)restNodes, (char*)page + insertOffset, restSize);

            memcpy((char*)page + insertOffset, key, keyLength);
            *(RID*)((char*)page + insertOffset + keyLength) = rid;
            int offset = insertOffset + keyLength + sizeof(RID);

            memcpy((char*)page + offset, (char*)restNodes, restSize);

            //modify freespace and entryCount;
            page->header.entryCount++;
            page->header.freeSpaceSize -= (keyLength + sizeof(RID));
            page->header.freeSpaceOffset += keyLength + sizeof(RID);
            page->header.lastEntryOffset += keyLength + sizeof(RID);

            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);

            free(restNodes);
        }else {
            // split leaf page
            // malloc new 2*pagesize space to copy original nodes + new node

            void *doubleSpace = malloc(PAGE_SIZE * 2);
            int offset = 0;

            //copy first part
            memcpy((char*)doubleSpace, (char*)page, insertOffset);
            offset += insertOffset;

            //copy inserted key
            memcpy((char*)doubleSpace + offset, key, keyLength);
            offset += keyLength;
            *(RID*)((char*)doubleSpace + offset) = rid;
            offset += sizeof(RID);

            //copy rest part
            memcpy((char*)doubleSpace + offset, (char*)page + insertOffset, restSize);

            int allEntryCount = page->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            //calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            int firstHalfOffset = 0;
            int prevFirstHalfOffset = 0;
            for (int i = 0; i < halfEntryCount; i++) {
                prevFirstHalfOffset = firstHalfOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                firstHalfOffset += curKeyLength + sizeof(RID);
            }

            //calculate offset of another half
            int totalOffset = firstHalfOffset;
            int prevTotalOffset = firstHalfOffset;
            for (int i = 0; i < allEntryCount - halfEntryCount; i++) {
                prevTotalOffset = totalOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
                totalOffset += curKeyLength + sizeof(RID);
            }

            // copy the firstHalf from doublespace to page
            memcpy((char*)page->data, (char*)doubleSpace, firstHalfOffset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy((char*)newPage->data, (char*)doubleSpace + firstHalfOffset, totalOffset - firstHalfOffset);

            //modify header of new page--------------------
            newPage->header.entryCount = allEntryCount - halfEntryCount;
            newPage->header.attrType = page->header.attrType;
            newPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - firstHalfOffset;
            newPage->header.freeSpaceSize = PAGE_SIZE - (totalOffset - firstHalfOffset) - sizeof(IXPageHeader);
            newPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();
            newPage->header.leftmostPtr = 0;
            newPage->header.prevPageNum = page->header.pageNum;
            newPage->header.nextPageNum = page->header.nextPageNum;
            newPage->header.parent = page->header.parent;
            newPage->header.lastEntryOffset = prevTotalOffset - firstHalfOffset + sizeof(IXPageHeader);
            newPage->header.pageType = LEAF_PAGE_TYPE;
            newPage->header.isRoot = false;

            ixfileHandle.appendPage(newPage);


            //modify header of page ------------------------
            page->header.entryCount = halfEntryCount;
            page->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
            page->header.freeSpaceSize = PAGE_SIZE - firstHalfOffset - sizeof(IXPageHeader);;
            page->header.nextPageNum = newPage->header.pageNum;
            page->header.lastEntryOffset = prevFirstHalfOffset + sizeof(IXPageHeader);

            //compose newChildEntry that will be returned
            // newChildEntry : key + PID
            void *willReturn = malloc(keyLength + sizeof(int));
            memcpy((char*)willReturn, (char*)key, keyLength);
            *(int*)((char*)willReturn + keyLength) = page->header.pageNum;
            newChildEntry = willReturn;

            ixfileHandle.writePage(newPage->header.pageNum, newPage);
            ixfileHandle.writePage(page->header.pageNum, page);

            delete(newPage);
            //free(willReturn);
            free(doubleSpace);
        }

        // if the page is not leaf page
    }else {
        IXPage *nextPage = findNextPage(ixfileHandle, page, key);

        insertTree(ixfileHandle, nextPage, key, rid, newChildEntry); //recursively insert entry

        // usual case, didn't split child
        if (newChildEntry == NULL) {
            delete(nextPage);
            return;
        }

        // split child, must insert newchildentry
        int keyLength = key_length(page->header.attrType, key);
        int insertSize = keyLength + sizeof(int);
        bool needSplit = false;

        if (page->header.freeSpaceSize - insertSize < 0) {
            needSplit = true;
        }

        int countNode = 0;
        // count the first half entry

        int insertOffset = findInsertOffset(page, key, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset;

        // if no split
        if (!needSplit) {

            void *restNodes = malloc(restSize);

            // copy rest part to restNodes
            memcpy((char*)restNodes, (char*)page + insertOffset, restSize);

            // copy newChildEntry to page
            int newChildEntryKeyLength = key_length(page->header.attrType, newChildEntry);
            memcpy((char*)page + insertOffset, (char*)newChildEntry, newChildEntryKeyLength);
            *(int*)((char*)page + insertOffset + newChildEntryKeyLength) = *(int*)((char*)newChildEntry +
                                                                                       newChildEntryKeyLength);

            // copy rest nodes from restNodes to page
            int offset = insertOffset + newChildEntryKeyLength + sizeof(int);
            memcpy((char*)page + offset, (char*)restNodes, restSize);

            // adjust freespace, entrycount, lastEntryOffset in page
            page->header.entryCount += 1;
            page->header.freeSpaceSize -= (newChildEntryKeyLength + sizeof(int));
            page->header.freeSpaceOffset += newChildEntryKeyLength + sizeof(int);
            page->header.lastEntryOffset += newChildEntryKeyLength + sizeof(int);

            free(newChildEntry);
            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);

            free(restNodes);

        }else {
            // need to split non-leaf node

            int allEntryCount = page->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            //malloc new space to copy original page + inserted key
            void *doubleSpace = malloc(PAGE_SIZE * 2);

            int offset = 0;  // offset in doubleSpace
            // copy first half
            memcpy((char*)doubleSpace + offset, (char*)page, insertOffset);
            offset += insertOffset;

            // copy newChildEntry
            int newChildEntryKeyLength = key_length(page->header.attrType, newChildEntry);
            memcpy((char*)doubleSpace + offset, (char*)newChildEntry, newChildEntryKeyLength);
            offset += newChildEntryKeyLength;
            *(int*)((char*)doubleSpace + offset) = *(int*)((char*)newChildEntry + newChildEntryKeyLength);
            offset += sizeof(int);

            //copy another half
            memcpy((char*)doubleSpace + offset, (char*)page + insertOffset, restSize);

            //calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            int firstHalfOffset = 0;
            int prevFirstHalfOffset = 0;
            for (int i = 0; i < halfEntryCount; i++) {
                prevFirstHalfOffset = firstHalfOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                firstHalfOffset += curKeyLength + sizeof(int);
            }

            //calculate offset of another half
            int totalOffset = firstHalfOffset;
            int prevTotalOffset = firstHalfOffset;

            //compose newChildEntry that will be returned
            // newChildEntry : key + PID
            int returnedKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
            int returnedPID = *(int*)((char*)doubleSpace + totalOffset + returnedKeyLength);

            void *willReturn = malloc(returnedKeyLength + sizeof(int));
            memcpy((char*)willReturn, (char*)doubleSpace + totalOffset, returnedKeyLength);
            *(int*)((char*)willReturn + returnedKeyLength) = returnedPID;
            newChildEntry = willReturn;

            totalOffset += returnedKeyLength + sizeof(int);
            int secondHalfBeginOffset = totalOffset;

            //calculate offset of another half except the returned entry
            for (int i = 0; i < allEntryCount - halfEntryCount - 1; i++) {
                prevTotalOffset = totalOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
                totalOffset += curKeyLength + sizeof(int);
            }

            // copy the firstHalf from doublespace to page
            memcpy((char*)page->data, (char*)doubleSpace, firstHalfOffset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy((char*)newPage->data, (char*)doubleSpace + secondHalfBeginOffset, totalOffset - secondHalfBeginOffset);

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


            //modify header of page ------------------------
            page->header.entryCount = halfEntryCount;
            page->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
            page->header.freeSpaceSize = PAGE_SIZE - firstHalfOffset - sizeof(IXPageHeader);;
            page->header.nextPageNum = -1;
            page->header.lastEntryOffset = prevFirstHalfOffset + sizeof(IXPageHeader);

            // if current page is the root page, create a new root index page
            if (page->header.isRoot) {
                IXPage *newRootPage = new IXPage;

                newChildEntryKeyLength = key_length(page->header.attrType, newChildEntry);
                memcpy((char*)newRootPage, (char*)newChildEntry, newChildEntryKeyLength);
                *(int*)((char*)newRootPage + newChildEntryKeyLength) = newPage->header.pageNum;

                newRootPage->header.leftmostPtr = page->header.pageNum;
                newRootPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();
                newRootPage->header.attrType = page->header.attrType;
                newRootPage->header.entryCount = 1;
                newRootPage->header.freeSpaceOffset = sizeof(IXPageHeader) + newChildEntryKeyLength + sizeof(int);
                newRootPage->header.freeSpaceSize = PAGE_SIZE - newRootPage->header.freeSpaceOffset;
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

// find non leaf page one by one level down
IXPage* IndexManager::findNextPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key) {
    if (page->header.entryCount == 0) {
        return page;
    }

    int nextPageNum = 0;
    IXPage *nextPage = new IXPage;

    // if key < firstKey, skip
    void* firstKey = page->data;

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

//find insert offset within a page
int IndexManager::findInsertOffset(IXPage *page, const void *key, int &countNode) {

    if (page->header.entryCount == 0) {
        return sizeof(IXPageHeader);
    }

    int ridOrPidSize = 0;

    if (page->header.pageType == LEAF_PAGE_TYPE) {
        ridOrPidSize = sizeof(RID);
    }else {
        ridOrPidSize = sizeof(int);
    }

    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;

    void *firstKey = (char*)page->data;
    void *lastKey = (char*)page + page->header.lastEntryOffset;

    // key < firstKey

    if (compareKey(key, firstKey, page->header.attrType) < 0) {
        countNode = 0;
        return sizeof(IXPageHeader);
    }

    //key >= lastKey
    if (compareKey(key, lastKey, page->header.attrType) >= 0) {
        countNode = entryCount;
        return page->header.freeSpaceOffset;
    }

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount - 1; i++) {
        void *curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        void *nextKey = (char*)page + offset + curKeyLength + ridOrPidSize;
        //int nextKeyLength = key_length(page->header.attrType, nextKey);
        countNode++;

        if (compareKey(curKey, key, page->header.attrType) <= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            return offset + curKeyLength + ridOrPidSize;
        }
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
    void* firstKey = page->data;
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
        void* curKey = page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        void* nextKey = page + offset + curKeyLength + sizeof(int);

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









