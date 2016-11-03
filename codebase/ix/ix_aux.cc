
#include "ix.h"


int IndexManager::compareKey(const void *key1, const void *key2, const AttrType attrType) {
    if (attrType == TypeInt) {
        return *(int*)key1 - *(int*)key2;
    }
    else if (attrType == TypeReal) {
        return *(float*)key1 - *(float*)key2;
    } else {
        int length1 = *(int*)key1;
        int length2 = *(int*)key2;

        char *str1 = (char*)malloc(lenght1 + 1);
        char *str2 = (char*)malloc(lenght2 + 1);

        memcpy(str1, (char*)key1 + length1, length1);
        str1[length1] = '\0';
        memcpy(str2, (char*)key2 + length2, length2);
        str2[length2] = '\0';

        return strcmp(str1, str2);
    }
}


IXPage* IndexManager::initializeIXPage(unsigned pageNum, char pageType, AttrType attrType) {
    IXPage *newPage = new IXPage;

    newPage.header.pageNum = pageNum;

    // initialize pointers to invalid values
    newPage->header.leftmostPtr = 0;
    newPage->header.prevPage = 0;
    newPage->header.nextPage = 0;

    newPage->header.freeSpaceSize = PAGE_SIZE - sizeof(IXPageHeader);
    newPage->header.freeSpaceOffset = sizeof(IXPageHeader);

    newPage->header.pageType = 0;
    newPage->header.pageType |= pageType;

    newPage->header.attrType = (char)attrType;

    newPage->header.entryCount = 0;

    return newPage;
}


RC IndexManager::initializeIndex(IXFileHandle &ixfileHandle, const int attrType) {

    // insert directory page, set attribute type
    IXPage *dirPage = initializeIXpage(0, DIR_PAGE_TYPE, attrType);
    dirPage->header.leftmostPtr = 1;                     // set next page as root page
    ixfileHandle.appendPage(dirPage);
    delete(dirPage);


    // insert root index page
    IXPage *rootPage = initializeIXPage(1, ROOT_PAGE_TYPE | INDEX_PAGE_TYPE, attrType);
    insertIndexEntryToIndexPage();
    ixfileHandle.appendPage(rootPage);
    delete(rootPage);

}


void IndexManager::readIndexEntryFromIndexPage(void *entry, void *page, short pos) {
    int keyLength = page->header.attrType == TypeVarChar? sizeof(int);

    short entryOffset = (char*) page + sizeof(IXPageHeader) +
    if () {
        int varCharLen = *(int*)entry;

    }

    memcpy(entry, (char*)page + sizeof(IXPageHeader), sizeof(int) + varCharLen);
}





IXPage * IndexManager::findLeafPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key, const RID &rid) {

    // case 1: input page is a leaf page
    if (page->header.pageType & LEAF_PAGE_TYPE != 0) {
        return indexPage;
    }

    // case 2: no leaf pages yet, create an empty one and return it
    if (page->header.entryCount == 0) {
        IXPage *leafPage = initializeIXPage(2, LEAF_PAGE_TYPE, indexPage->header.attrType);
        insertDataEntryToLeafPage(leafPage, key, rid);
        leafPage->header.entryCount++;
        ixfileHandle.appendPage(leafPage);
        return leafPage;
    }


    // case 2: compare the keys sequentially
    void* curEntry = malloc(PAGE_SIZE);


    if (compareKey(key, , page->header.attrType) >= 0) {



    }

    ixfileHandle.readPage();
    return findLeafPage(ixfileHandle, );


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









