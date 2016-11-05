
#include "ix.h"
#include "../rbf/rbfm.h"


IX_ScanIterator::IX_ScanIterator() {
    isEOF = false;
    open = false;
}

IX_ScanIterator::~IX_ScanIterator() {

}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

    if (!open) {
        if (DEBUG) printf("ixsi is not open!\n");
        return -1;
    }

    if (isEOF) {
        if (DEBUG) printf("EOF is reached\n");
        return -1;
    }



    // fetch the leaf page indicated by the nextEid
    IXPage *targetLeafPage = new IXPage;
    ixfh.readPage(nextEid.pageNum, targetLeafPage);

    // locate the target data entry
    char* entryPtr = (char*)targetLeafPage + sizeof(IXPageHeader);
    unsigned entryNum = 0;
    while (entryNum < nextEid.slotNum) {
        int keyLen = attrType == TypeVarChar ? sizeof(int) + *(int*)entryPtr : sizeof(int);
        entryPtr += keyLen + sizeof(RID);
        entryNum++;
    }



    // if highkey == NULL, scan all the records after low key
    // otherwise check ending condition
    if (highKey != NULL) {
        IndexManager *ixm = IndexManager::instance();
        int cmpResult = ixm->compareKey(entryPtr, highKey, attrType);
        // entryKey > high key or entryKey == high key but not inclusive, return EOF
        if (cmpResult > 0 || (cmpResult == 0 && !highKeyInclusive)) {
            delete targetLeafPage;
            return IX_EOF;
        }
    }




    // find key length of the target entry and copy entry out
    int entryKeyLen = attrType == TypeVarChar ? sizeof(int) + *(int*)entryPtr : sizeof(int);
    memcpy(key, entryPtr, (size_t)entryKeyLen);
    memcpy(&rid, entryPtr + entryKeyLen, sizeof(RID));


    nextEid.slotNum++;
    if (nextEid.slotNum == targetLeafPage->header.entryCount) {

        // if this page is the last leaf page, flag IX_EOF, so next call will return EOF
        if (targetLeafPage->header.nextPageNum == targetLeafPage->header.pageNum) {
            isEOF = true;
            delete targetLeafPage;
            return 0;
        }
        nextEid.pageNum = targetLeafPage->header.nextPageNum;
        nextEid.slotNum = 0;
    }

    delete targetLeafPage;
    return 0;
}

RC IX_ScanIterator::close() {

    IndexManager *ixm = IndexManager::instance();
    open = false;
    return 0;
}
