
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


    // fetch the leaf page
    IXPage *targetLeafPage = new IXPage;
    ixfh.readPage(pageOfNextEntry, targetLeafPage);


    // first time reading leaf page
    if (latestFreeSpaceSize < 0 || offsetOfNextEntry == 0) {
        offsetOfCurrentEntry = offsetOfNextEntry;
    }
    else if (latestFreeSpaceSize != targetLeafPage->header.freeSpaceSize) {
        offsetOfNextEntry = offsetOfCurrentEntry;
    }

    latestFreeSpaceSize = targetLeafPage->header.freeSpaceSize;

    // if reach the end of target leaf page
    if (offsetOfNextEntry == targetLeafPage->header.freeSpaceOffset - sizeof(IXPageHeader)) {
        // if this is the last leaf page
        if (targetLeafPage->header.nextPageNum == targetLeafPage->header.pageNum) {
            return IX_EOF;
        }

        // getNext on next page
        pageOfNextEntry = targetLeafPage->header.nextPageNum;
        offsetOfNextEntry = 0;
        delete(targetLeafPage);
        return getNextEntry(rid, key);
    }


    // locate the target data entry
    IndexManager *ixm = IndexManager::instance();
    char* entryPtr = targetLeafPage->data + offsetOfNextEntry;
    int keyLen = ixm->key_length(attrType, entryPtr);

    // search for the first key, only used when first time call getNext and lowKey != NULL
    if (lowKey) {
        int cmpResult = ixm->compareKey(entryPtr, lowKey, attrType);
        if (cmpResult < 0 || (cmpResult == 0 && !lowKeyInclusive)){
            delete(targetLeafPage);
            offsetOfCurrentEntry = offsetOfNextEntry;
            offsetOfNextEntry += keyLen + sizeof(RID);
            return getNextEntry(rid, key);
        }
    }



    // if highkey == NULL, scan all the records after low key
    // otherwise check ending condition
    if (highKey != NULL) {
        int cmpResult = ixm->compareKey(entryPtr, highKey, attrType);
        // entryKey > high key or entryKey == high key but not inclusive, return EOF
        if (cmpResult > 0 || (cmpResult == 0 && !highKeyInclusive)) {
            delete targetLeafPage;
            return IX_EOF;
        }
    }

    // find key length of the target entry and copy entry out
    memcpy(key, entryPtr, (size_t)keyLen);
    memcpy(&rid, entryPtr + keyLen, sizeof(RID));


    offsetOfCurrentEntry = offsetOfNextEntry;
    offsetOfNextEntry += keyLen + sizeof(RID);
    delete targetLeafPage;
    return 0;
}

RC IX_ScanIterator::close() {

    IndexManager *ixm = IndexManager::instance();
    open = false;
    return 0;
}
