
#include "ix.h"

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

    IXPage *rootPage = new IXPage;
    ixfileHandle.readPage(dirPage->header.leftmostPtr, rootPage);

    if (rootPage->header.entryCount == 0) {

        insertEntryToEmptyRoot(ixfileHandle, rootPage, key, rid);

    }else {
        insertTree(ixfileHandle, rootPage, key, rid, NULL);
    }

    delete(dirPage);
    delete(rootPage);

    return 0;
}




RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator) {


    // initialize ixsi
    ix_ScanIterator.open = true;
    ix_ScanIterator.ixfh = ixfileHandle;
    ix_ScanIterator.attrType = attribute.type;


    // fetch directory page
    IXPage *dirPage = new IXPage;
    ixfileHandle.readPage(0, dirPage);
    unsigned rootPageNum = dirPage->header.leftmostPtr;
    delete dirPage;

    // fetch root page
    IXPage *rootPage = new IXPage;
    ixfileHandle.readPage(rootPageNum, rootPage);

    // fetch the leaf page that MAY contain the data entry
    IXPage *targetLeafPage = findLeafPage(ixfileHandle, rootPage, lowKey);
    delete rootPage;

    // search for the data entry satisfying low key
    char* entryPtr = (char*)targetLeafPage + sizeof(IXPageHeader);
    unsigned entryNum = 0;
    while (entryNum < targetLeafPage->header.entryCount && compareKey(entryPtr, lowKey, attribute.type) < 0) {

        int keyLen = attribute.type == TypeVarChar ? sizeof(int) + *(int*)entryPtr : sizeof(int);
        entryPtr += keyLen + sizeof(RID);
        entryNum++;
    }



    RID tmpNextEid = {};

    // if all data entries on this leaf page have been examined, set 0th entry on next page as the next data entry
    if (entryNum == targetLeafPage->header.entryCount) {
        tmpNextEid.pageNum = targetLeafPage->header.nextPageNum;
        tmpNextEid.slotNum = 0;
    }
    // else the matched data entry is on this page
    else {

        // decide if this data entry can considered as the next data entry
        if  (compareKey(entryPtr, lowKey, attribute.type) == 0 && !lowKeyInclusive) {
            entryNum++;
        }
        tmpNextEid.pageNum = targetLeafPage->header.pageNum;
        tmpNextEid.slotNum = entryNum;
    }

    ix_ScanIterator.nextEid.pageNum = tmpNextEid.pageNum;
    ix_ScanIterator.nextEid.slotNum = tmpNextEid.slotNum;


    return 0;

}



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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

