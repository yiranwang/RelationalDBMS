
#include <cstdlib>
#include "ix.h"
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
    }else if (pageType == INDEX_PAGE_TYPE) {
        newPage->header.parent = 1;
    }else if (pageType == LEAF_PAGE_TYPE) {
        newPage->header.parent = 1;
    }

    // initialize pointers to invalid values
    newPage->header.leftmostPtr = 0;
    newPage->header.prevPageNum = 0;
    newPage->header.nextPageNum = 0;

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
}

void IndexManager::insertTree(IXFileHandle &ixfileHandle, IXPage *page, const void *key, const RID &rid,
                              void* newChildEntry) {
    IXPage *insertPage = findLeafPage(ixfileHandle, page, key);
    int pageType = insertPage->header.pageType;

    int keyLength = key_length(insertPage->header.attrType, key);
    int insertSize = keyLength + sizeof(RID);
    bool needSplit = false;

    if (insertPage->header.freeSpaceSize - insertSize < 0) {
        needSplit = true;
    }

    // if the page to insert is leaf page
    if (pageType == LEAF_PAGE_TYPE) {
        int countNode = 0;
        // find insert offset in insertPage
        int insertOffset = findInsertOffset(insertPage, key, countNode);
        int restSize = insertPage->header.freeSpaceOffset - insertOffset;

        // don't split leaf page
        if (!needSplit) {
            // copy the rest leaf nodes to restNodes
            void *restNodes = malloc(restSize);
            memcpy((char*)restNodes, (char*)insertPage->data + insertOffset, restSize);

            memcpy((char*)insertPage->data + insertOffset, key, keyLength);
            *(RID*)((char*)insertPage->data + insertOffset + keyLength) = rid;
            int offset = insertOffset + keyLength + sizeof(RID);

            memcpy((char*)insertPage->data + offset, (char*)restNodes, restSize);

            //modify freespace and entryCount;
            insertPage->header.entryCount++;
            insertPage->header.freeSpaceSize -= (keyLength + sizeof(RID));
            insertPage->header.freeSpaceOffset += keyLength + sizeof(RID);

            newChildEntry = NULL;

            free(restNodes);
        }else {
            // split leaf page
            // malloc new 2*pagesize space to copy original nodes + new node

            void *doubleSpace = malloc(PAGE_SIZE * 2);
            int offset = 0;

            //copy first part
            memcpy((char*)doubleSpace, (char*)insertPage->data, insertOffset);
            offset += insertOffset;

            //copy inserted key
            memcpy((char*)doubleSpace + offset, key, keyLength);
            offset += keyLength;
            *(RID*)((char*)doubleSpace + offset) = rid;
            offset += sizeof(RID);

            //copy rest part
            memcpy((char*)doubleSpace + offset, (char*)insertPage->data + insertOffset - sizeof(IXPageHeader), restSize);

            int allEntryCount = insertPage->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            offset = 0;
            int prevOffset = 0;
            for (int i = 0; i < halfEntryCount; i++) {
                prevOffset = offset;
                int curKeyLength = key_length(insertPage->header.attrType, doubleSpace + offset);
                offset += curKeyLength + sizeof(RID);
            }

            int secondPartOffset = offset;
            int secondPartPrevOffset = offset;
            for (int i = 0; i < allEntryCount - halfEntryCount; i++) {
                secondPartPrevOffset = secondPartOffset;
                int curKeyLength = key_length(insertPage->header.attrType, doubleSpace + secondPartOffset);
                secondPartOffset += curKeyLength + sizeof(RID);
            }

            memcpy((char*)insertPage->data, (char*)doubleSpace, offset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy((char*)newPage->data, (char*)doubleSpace + offset, secondPartOffset - offset);

            //modify header of new page--------------------
            newPage->header.entryCount = allEntryCount - halfEntryCount;
            newPage->header.attrType = insertPage->header.attrType;
            newPage->header.freeSpaceOffset = sizeof(IXPageHeader) + secondPartOffset - offset;
            newPage->header.freeSpaceSize = PAGE_SIZE - (secondPartOffset - offset) - sizeof(IXPageHeader);
            newPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages() + 1;
            newPage->header.leftmostPtr = -1;
            newPage->header.prevPageNum = insertPage->header.pageNum;
            newPage->header.nextPageNum = insertPage->header.nextPageNum;
            newPage->header.parent = insertPage->header.parent;
            newPage->header.lastEntryOffset = secondPartPrevOffset - offset + sizeof(IXPageHeader);

            ixfileHandle.writePage(newPage->header.pageNum, newPage);


            //modify header of insertPage ------------------------
            insertPage->header.entryCount = halfEntryCount;
            insertPage->header.freeSpaceOffset = sizeof(IXPageHeader) + offset;
            insertPage->header.freeSpaceSize = PAGE_SIZE - offset - sizeof(IXPageHeader);;
            insertPage->header.nextPageNum = newPage->header.pageNum;
            insertPage->header.lastEntryOffset = prevOffset + sizeof(IXPageHeader);

            //compose newChildEntry that will be returned
            void *willReturn = malloc(keyLength + sizeof(RID));
            memcpy((char*)willReturn, key, keyLength);
            *(RID*)((char*)willReturn + keyLength) = rid;
            newChildEntry = willReturn;

            free(newPage);
            free(willReturn);
            free(doubleSpace);
        }

        // if the page is not leaf page
    }else {

    }

}


int IndexManager::findInsertOffset(IXPage *page, const void *key, int &countNode) {

    int ridOrPidSize = 0;

    if (page->header.pageType == LEAF_PAGE_TYPE) {
        ridOrPidSize = sizeof(RID);
    }else {
        ridOrPidSize = sizeof(int);
    }

    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;
    int offset = 0;

    void *firstKey = page->data;
    void *lastKey = page->data + page->header.lastEntryOffset;

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

    for (int i = 0; i < entryCount - 1; i++) {
        void *curKey = page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        void *nextKey = page + offset + curKeyLength + ridOrPidSize;
        //int nextKeyLength = key_length(page->header.attrType, nextKey);
        countNode++;
        if (compareKey(curKey, key, page->header.attrType) <= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            return offset + curKeyLength + ridOrPidSize;
        }
        offset += curKeyLength + ridOrPidSize;
    }

    return -1;
}


void IndexManager::readIndexEntryFromIndexPage(void *entry, void *page, short pos) {
    int keyLength = page->header.attrType == TypeVarChar? sizeof(int);

    short entryOffset = (char*) page + sizeof(IXPageHeader) +
    if () {
        int varCharLen = *(int*)entry;

    }

    memcpy(entry, (char*)page + sizeof(IXPageHeader), sizeof(int) + varCharLen);
}





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

    void* curEntry = malloc(PAGE_SIZE);

    // if key < firstKey, skip
    void* firstKey = page->data;
    if (compareKey(key, firstKey, page->header.attrType) < 0) {

        nextPageNum = page->header.leftmostPtr;

        ixfileHandle.readPage(nextPageNum, nextPage);

        return findLeafPage(ixfileHandle, nextPage, key);
    }

    // if key >= lastKey, skip
    void* lastKey = (char*)page->data + page->header.lastEntryOffset;
    if (compareKey(key, lastKey, page->header.attrType) >= 0) {

        nextPageNum = *(int*)((char*)page + PAGE_SIZE - sizeof(int));
        ixfileHandle.readPage(nextPageNum, nextPage);
        return findLeafPage(ixfileHandle, nextPage, key);
    }

    // if firstKey < key < lastKey
    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;
    int offset = 0;

    char* curPage = page->data;

    for (int i = 0; i < entryCount - 1; i++) {
        void* curKey = curPage + offset;
        void* nextKey = curPage + offset + keyLength + sizeof(int);

        if (compareKey(key, curKey, page->header.attrType) >= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            nextPageNum = *(int*)(curPage + offset + keyLength);
            ixfileHandle.readPage(nextPageNum, nextPage);
            return findLeafPage(ixfileHandle, nextPage, key);
        }
        offset += keyLength + sizeof(int);
    }

    free(nextPage);

    return NULL;

}



// data entry on leaf page: <key, rid>
void IndexManager::insertDataEntryToLeafPage(IXPage *leafPage, const void *key, const RID &rid) {
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

}









