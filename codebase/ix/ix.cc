
#include "ix.h"

using namespace std;

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
    return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName) {
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
    return PagedFileManager::instance()->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
    return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}




RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {

    // if index file is empty, initialize the index file
    unsigned pageCount = ixfileHandle.fileHandle.getNumberOfPages();
    if (pageCount == 0) {
        initializeIndex(ixfileHandle, attribute.type);
    }

    IXPage *dirPage = new IXPage;
    ixfileHandle.readPage(0, dirPage);

    int rootPageNumber = dirPage->header.leftmostPtr;

    IXPage *rootPage = new IXPage;
    ixfileHandle.readPage(rootPageNumber, rootPage);

    delete(dirPage);

    if (rootPage->header.entryCount == 0) {

        insertEntryToEmptyRoot(ixfileHandle, rootPage, key, rid);

    }else {

        void *newChildEntry = NULL;
        insertTree(ixfileHandle, rootPage, key, rid, newChildEntry);
    }


    delete(rootPage);

    return 0;
}



RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {

    IXPage *dirPage = new IXPage;
    ixfileHandle.readPage(0, dirPage);

    unsigned rootPageNumber = dirPage->header.leftmostPtr;
    delete(dirPage);

    IXPage *rootPage = new IXPage;
    ixfileHandle.readPage(rootPageNumber, rootPage);


    void *newChildEntry = NULL;

    int rc = deleteTree(ixfileHandle, rootPage, key, rid, newChildEntry);

    delete(rootPage);

    return rc;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator) {

    // validate fileHandle
    if (ixfileHandle.fileHandle.fd < 0) {
        return -1;
    }


    // initialize ixsi
    ix_ScanIterator.open = true;
    ix_ScanIterator.ixfh = ixfileHandle;
    ix_ScanIterator.attrType = attribute.type;
    ix_ScanIterator.latestFreeSpaceSize = -1;
    ix_ScanIterator.pageOfNextEntry = 0;
    ix_ScanIterator.offsetOfCurrentEntry = -1;


    // copy lowKey
    if (!lowKey) {
        ix_ScanIterator.lowKey = NULL;
    } else {
        size_t keyLen = attribute.type == TypeVarChar ? *(int*)lowKey + sizeof(int) : sizeof(int);
        ix_ScanIterator.lowKey = malloc(keyLen);
        memcpy(ix_ScanIterator.lowKey, lowKey, keyLen);
    }

    // copy highKey
    if (!highKey) {
        ix_ScanIterator.highKey = NULL;
    } else {
        size_t keyLen = attribute.type == TypeVarChar ? *(int*)highKey + sizeof(int) : sizeof(int);
        ix_ScanIterator.highKey = malloc(keyLen);
        memcpy(ix_ScanIterator.highKey, highKey, keyLen);
    }

    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;


    // fetch directory page, get root pageNum
    IXPage *dirPage = new IXPage;
    ixfileHandle.readPage(0, dirPage);
    unsigned rootPageNum = dirPage->header.leftmostPtr;
    delete dirPage;

    // fetch root page
    IXPage *rootPage = new IXPage;
    ixfileHandle.readPage(rootPageNum, rootPage);

    if (rootPage->header.entryCount == 0) {
        delete(rootPage);
        return 0;
    }

    // locate first leaf page if lowKey == NULL
    if (lowKey == NULL) {
        IXPage *targetLeafPage = findFirstLeafPage(ixfileHandle, rootPage);
        ix_ScanIterator.pageOfNextEntry = targetLeafPage->header.pageNum;
        ix_ScanIterator.offsetOfNextEntry = 0;
        delete targetLeafPage;
        return 0;
    }

    // otherwise fetch the leaf page that MAY contain the data entry
    else {
        IXPage *targetLeafPage = findLeafPage(ixfileHandle, rootPage, lowKey);
        ix_ScanIterator.pageOfNextEntry = targetLeafPage->header.pageNum;
        ix_ScanIterator.offsetOfNextEntry = 0;

        // search for the data entry satisfying low key, offset counted from page->data
        char *entryPtr = targetLeafPage->data;
        unsigned entryNum = 0;

        // find the first key >= lowKey
        while (entryNum < targetLeafPage->header.entryCount && compareKey(entryPtr, lowKey, attribute.type) < 0) {
            int keyLen = attribute.type == TypeVarChar ? sizeof(int) + *(int *) entryPtr : sizeof(int);
            entryPtr += keyLen + sizeof(RID);
            entryNum++;
            ix_ScanIterator.offsetOfNextEntry += keyLen + sizeof(RID);
        }

        // if all data entries on this leaf page have been examined
        if (entryNum == targetLeafPage->header.entryCount) {
            ix_ScanIterator.pageOfNextEntry = targetLeafPage->header.nextPageNum;
            ix_ScanIterator.offsetOfNextEntry = 0;
        }

        delete targetLeafPage;
        return 0;
    }

}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    // fetch directory page
    IXPage *dirPage = new IXPage;
    ixfileHandle.readPage(0, dirPage);
    int rootPageNum = dirPage->header.leftmostPtr;

    DFSPrintBTree(rootPageNum, ixfileHandle, attribute);
    printf("\n");
    delete dirPage;
}



IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;

}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);

    ixReadPageCounter = readPageCount;
    ixWritePageCounter = writePageCount;
    ixAppendPageCounter = appendPageCount;
    return 0;
}

