
#include "ix.h"
#include "../rbf/rbfm.h"


IX_ScanIterator::IX_ScanIterator() {
    open = false;
}

IX_ScanIterator::~IX_ScanIterator() {

}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

    if (!open) {
        if (DEBUG) printf("ixsi is not open!\n");
        return -1;
    }
    unsigned totalPageNum = ixfh.fileHandle.getNumberOfPages();
    if (nextEid.pageNum == totalPageNum) {
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

    // find key length of the target entry
    int entryKeyLen = attrType == TypeVarChar ? sizeof(int) + *(int*)entryPtr : sizeof(int);
    memcpy(key, entryPtr, (size_t)entryKeyLen);
    memcpy(&rid, (char*)entryKeyLen + entryKeyLen, sizeof(RID));


    nextEid.slotNum++;
    if (nextEid.slotNum == targetLeafPage->header.entryCount) {
        nextEid.pageNum = targetLeafPage->header.nextPageNum;
        nextEid.slotNum = 0;
    }

    delete targetLeafPage;
    return 0;
}

RC IX_ScanIterator::close() {

    IndexManager *ixm = IndexManager::instance();
    ixm->closeFile(ixfh);
    open = false;
    return 0;
}
