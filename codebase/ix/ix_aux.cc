
#include <cstdlib>
#include "ix.h"
#include "../rbf/rbfm.h"
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
        float k1 = *(float*)key1;
        float k2 = *(float*)key2;
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
        *(str1 + length1) = '\0';
        memcpy(str2, (char*)key2 + sizeof(int), length2);
        *(str2 + length2) = '\0';

        if (strcmp(str1, str2) < 0) {
            //printf("************** BUG str1: %s, str2: %s\n", str1, str2);
        }

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
        int dataEntryLength = keyLength + sizeof(RID);
        bool needSplit = page->header.freeSpaceSize - dataEntryLength < 0;

        int countNode = 0;
        // find insert offset in page->data(!!!!! don't include IXPageHeader size!!!!!!!!!!)
        int insertOffset = findInsertOffset(page, key, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset - sizeof(IXPageHeader);

        // case 1.1: enough space in leaf page, so no need to split leaf page
        if (!needSplit) {

            // insert the data entry
            void *restNodes = malloc((size_t)restSize);
            int offset = insertOffset + sizeof(IXPageHeader);
            memcpy((char*)restNodes, (char*)page + offset, (size_t)restSize);

            memcpy((char*)page + offset, key, (size_t)keyLength);
            offset += keyLength;

            *(RID*)((char*)page + offset) = rid;
            offset += sizeof(RID);

            memcpy((char*)page + offset, (char*)restNodes, (size_t)restSize);
            free(restNodes);

            // if inserted data entry is the last one, shift lastEntryOffset to the right by lastEntryLength
            // otherwise shift it to the right by inserted entry's length

            if (countNode == page->header.entryCount) {
                page->header.lastEntryOffset = page->header.freeSpaceOffset;
            } else {
                page->header.lastEntryOffset += dataEntryLength;
            }



            page->header.entryCount++;
            page->header.freeSpaceSize -= dataEntryLength;
            page->header.freeSpaceOffset += dataEntryLength;

            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);

        }
        // case 1.2: not enough space in leaf page, need to split this leaf page
        else {
            if (DEBUG) printf("Splitting a leaf...\n");
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
            offsetInDoubleSpace += restSize;

            // now  doubleSpace: [entry0, entry1, ..., entryInsert, ..., entryM, ..., \0, \0, \0,]

            int allEntryCount = page->header.entryCount + 1;
            int halfEntryCount = allEntryCount / 2;

            // calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            int firstHalfOffset = 0;                // offset of the second half's starting address (inside page->data)
            int prevFirstHalfOffset = 0;            // offset of the last entry in the first half (inside page->data)

            // handel space nicely for varchar key
            int firstHalfEntryCountForVarChar = 0;

            /*for (int i = 0; i < halfEntryCount; i++) {
                prevFirstHalfOffset = firstHalfOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                firstHalfOffset += curKeyLength + sizeof(RID);
            }*/

            if (page->header.attrType == TypeVarChar) {
                for (int i = 0; i < allEntryCount; i++) {
                    int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                    if (firstHalfOffset + curKeyLength  + sizeof(RID) > offsetInDoubleSpace / 2) {
                        break;
                    }
                    prevFirstHalfOffset = firstHalfOffset;
                    firstHalfEntryCountForVarChar++;
                    firstHalfOffset += curKeyLength + sizeof(RID);
                }
                //-----------------------------------------------------
            }else {
                for (int i = 0; i < halfEntryCount; i++) {
                    prevFirstHalfOffset = firstHalfOffset;
                    int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                    firstHalfOffset += curKeyLength + sizeof(RID);
                }
            }

            if (page->header.attrType == TypeVarChar) {
                halfEntryCount = firstHalfEntryCountForVarChar;
            }

            // compose newChildEntry that will be returned
            // (COPY UP) newChildEntry : key + PID
            int returnedKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
            void *willReturn = malloc(returnedKeyLength + sizeof(unsigned));
            memcpy((char*)willReturn, (char*)doubleSpace + firstHalfOffset, returnedKeyLength);
            *(unsigned*)((char*)willReturn + returnedKeyLength) = ixfileHandle.fileHandle.getNumberOfPages();
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

            // if current page is the last page
            if (page->header.nextPageNum == page->header.pageNum) {
                newPage->header.nextPageNum = newPage->header.pageNum;
            }else {
            // if current page is not the last page
                newPage->header.nextPageNum = page->header.nextPageNum;
            }
            //newPage->header.parent = page->header.parent;
            newPage->header.lastEntryOffset = prevTotalOffset - firstHalfOffset + sizeof(IXPageHeader);
            newPage->header.pageType = LEAF_PAGE_TYPE;
            newPage->header.isRoot = false;
            newPage->header.parent = page->header.parent;

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

        // case 2.2: child is split, must insert newchildentry <key, PID>
        int newChildEntryLength = key_length(page->header.attrType, newChildEntry) + sizeof(unsigned);
        bool needSplit = page->header.freeSpaceSize - newChildEntryLength < 0;


        int countNode = 0;

        // !!!!!insertOffset don't include IXPageHeader size!!!!!!!!!!!!!!
        int insertOffset = findInsertOffset(page, newChildEntry, countNode);
        int restSize = page->header.freeSpaceOffset - insertOffset - sizeof(IXPageHeader);

        // case 2.2.1: if no need to split this index page
        if (!needSplit) {


            // copy rest part out to restNodes
            int offset = sizeof(IXPageHeader) + insertOffset;
            void *restNodes = malloc(restSize);
            memcpy((char*)restNodes, (char*)page + offset, restSize);

            // insert newChildEntry to page
            memcpy((char*)page + offset, (char*)newChildEntry, newChildEntryLength);
            offset += newChildEntryLength;


            // copy rest nodes from restNodes back to page
            memcpy((char*)page + offset, (char*)restNodes, restSize);
            free(restNodes);


            if (countNode == page->header.entryCount) {
                page->header.lastEntryOffset = page->header.freeSpaceOffset;
            } else {
                page->header.lastEntryOffset +=newChildEntryLength;
            }

            // adjust freespace, entrycount, lastEntryOffset in page
            page->header.entryCount += 1;
            page->header.freeSpaceSize -= newChildEntryLength;
            page->header.freeSpaceOffset += newChildEntryLength;

            free(newChildEntry);
            newChildEntry = NULL;

            ixfileHandle.writePage(page->header.pageNum, page);



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
            offsetInDoubleSpace += restSize;

            int firstHalfOffset = 0;
            int prevFirstHalfOffset = 0;


            // locate the first half; calculate the offset of the first half, prevOffset is used to record the lastEntry offset
            // handel space nicely for varchar key
            int firstHalfEntryCountForVarChar = 0;
            if (page->header.attrType == TypeVarChar) {
                for (int i = 0; i < allEntryCount; i++) {
                    int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                    if (firstHalfOffset + curKeyLength > offsetInDoubleSpace / 2) {
                        break;
                    }
                    prevFirstHalfOffset = firstHalfOffset;
                    firstHalfEntryCountForVarChar++;
                    firstHalfOffset += curKeyLength + sizeof(int);
                }
            //-----------------------------------------------------
            }else {
                for (int i = 0; i < halfEntryCount; i++) {
                    prevFirstHalfOffset = firstHalfOffset;
                    int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + firstHalfOffset);
                    firstHalfOffset += curKeyLength + sizeof(int);
                }
            }

            if (page->header.attrType == TypeVarChar) {
                halfEntryCount = firstHalfEntryCountForVarChar;
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


            totalOffset += returnedKeyLength + sizeof(unsigned);
            int secondHalfBeginOffset = totalOffset;



            //calculate offset of the end and the last entry of the other half except the returned entry
            // handel space nicely for varchar key


            for (int i = 0; i < allEntryCount - halfEntryCount - 1; i++) {
                prevTotalOffset = totalOffset;
                int curKeyLength = key_length(page->header.attrType, (char*)doubleSpace + totalOffset);
                totalOffset += curKeyLength;

                // adjust the parents of children pages of new splitted index page to newPage
                int curEntryPid = *(int*)((char*)doubleSpace + totalOffset);
                IXPage *curEntryPidPage = new IXPage;
                ixfileHandle.readPage(curEntryPid, curEntryPidPage);
                curEntryPidPage->header.parent = ixfileHandle.fileHandle.getNumberOfPages();
                ixfileHandle.writePage(curEntryPid, curEntryPidPage);
                delete curEntryPidPage;
                //----------------------------------------------------------------------------

                totalOffset += sizeof(unsigned);
            }

            // copy the firstHalf from doublespace to page
            memcpy(page->data, (char*)doubleSpace, firstHalfOffset);

            //new a new page for second half of original nodes
            IXPage *newPage = new IXPage;

            memcpy(newPage->data, (char*)doubleSpace + secondHalfBeginOffset, totalOffset - secondHalfBeginOffset);

            //modify header of new page--------------------
            newPage->header.entryCount = allEntryCount - halfEntryCount - 1;
            newPage->header.pageType = INDEX_PAGE_TYPE;
            newPage->header.attrType = page->header.attrType;
            newPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - secondHalfBeginOffset;
            newPage->header.freeSpaceSize = PAGE_SIZE - (totalOffset - secondHalfBeginOffset) - sizeof(IXPageHeader);
            newPage->header.pageNum = ixfileHandle.fileHandle.getNumberOfPages();

            newPage->header.leftmostPtr = returnedPID;
            // adjust the leftMost children page's parentNum to newPage
            IXPage *leftMostPidPage = new IXPage;
            ixfileHandle.readPage(returnedPID, leftMostPidPage);
            leftMostPidPage->header.parent = newPage->header.pageNum;
            ixfileHandle.writePage(returnedPID, leftMostPidPage);
            delete leftMostPidPage;
            //----------------------------------------------------------------------------

            newPage->header.prevPageNum = 0;
            newPage->header.nextPageNum = 0;
            newPage->header.parent = page->header.parent;
            newPage->header.lastEntryOffset = prevTotalOffset - secondHalfBeginOffset + sizeof(IXPageHeader);
            newPage->header.isRoot = false;

            ixfileHandle.appendPage(newPage);

            // newChildEntry's pointer to newPage
            if (newChildEntry) {
                free(newChildEntry);
            }
            *(unsigned*)((char*)willReturn + returnedKeyLength) = newPage->header.pageNum;
            newChildEntry = willReturn;


            //modify header of the original page ------------------------
            page->header.entryCount = halfEntryCount;
            page->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
            page->header.freeSpaceSize = PAGE_SIZE - firstHalfOffset - sizeof(IXPageHeader);

            page->header.lastEntryOffset = prevFirstHalfOffset + sizeof(IXPageHeader);

            // if current page is the root page, create a new root index page
            if (page->header.isRoot) {
                IXPage *newRootPage = new IXPage;

                if (DEBUG) printf("Splitting root page:%u...\n", page->header.pageNum);

                // insert newChildEntry into new root page
                unsigned newChildEntryLength = returnedKeyLength + sizeof(unsigned);
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
                newPage->header.parent = newRootPage->header.pageNum;

                // adjust rootPage number in dirPage
                IXPage *dirPage = new IXPage;
                ixfileHandle.readPage(0, dirPage);
                dirPage->header.leftmostPtr = newRootPage->header.pageNum;
                ixfileHandle.writePage(0, dirPage);

                if (DEBUG) printf("************ Splitted a root: pageNum = %u!\n",page->header.pageNum );

                free(newChildEntry);
                newChildEntry = NULL;

                delete(newRootPage);
                delete(dirPage);
            }

            ixfileHandle.writePage(page->header.pageNum, page);
            ixfileHandle.writePage(newPage->header.pageNum, newPage);


            //free(newChildEntry);
            //free(willReturn);
            free(doubleSpace);
            delete(newPage);
        }

        delete(nextPage);
    }

}


// find the next child non leaf page one by one level down
IXPage* IndexManager::findNextPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key) {
    IXPage *nextPage = new IXPage;
    if (page->header.entryCount == 0) {
        memcpy(nextPage, page, PAGE_SIZE);
        return nextPage;
    }

    int nextPageNum = 0;
    // if key < firstKey, skip
    char* firstKey = (char*)page + sizeof(IXPageHeader);

    if (compareKey(key, firstKey, page->header.attrType) < 0) {

        nextPageNum = page->header.leftmostPtr;

        ixfileHandle.readPage(nextPageNum, nextPage);

        return nextPage;
    }

    // if key >= lastKey, skip
    void* lastKey = (char*)page + page->header.lastEntryOffset;
    int lastKeyVal = *(int*)((char*)lastKey);

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
            nextPageNum = *(unsigned*)((char*)page + offset + curKeyLength);
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

    for (int i = 0; i < entryCount; i++) {
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


// returns the input page
IXPage* IndexManager::findFirstLeafPage(IXFileHandle &ixfileHandle, IXPage *page) {
    // case 1: input page is a leaf page
    if (page->header.pageType == 1) {
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
// return the input page
IXPage * IndexManager::findLeafPage(IXFileHandle &ixfileHandle, IXPage *page, const void *key) {

    int pageType = page->header.pageType;

    // case 1: input page is a leaf page
    if (pageType == 1) {
        return page;
    }

    // case 2: input page is an index page
    // case 2: compare the keys sequentially


    // if key < firstKey, skip
    char* firstKey = page->data;
    if (compareKey(key, firstKey, page->header.attrType) < 0) {
        ixfileHandle.readPage(page->header.leftmostPtr, page);
        return findLeafPage(ixfileHandle, page, key);
    }

    // if firstKey < key < lastKey
    int keyLength = key_length(page->header.attrType, key);
    int entryCount = page->header.entryCount;

    int offset = 0;

    for (int i = 0; i < entryCount - 1; i++) {
        char* curKey = (char*)page->data + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);
        char* nextKey = (char*)page->data + offset + curKeyLength + sizeof(unsigned);

        if (compareKey(key, curKey, page->header.attrType) == 0) {
            unsigned childPageNum = *(unsigned*)((char*)page->data + offset + curKeyLength);
            ixfileHandle.readPage(childPageNum, page);
            return findLeafPage(ixfileHandle, page, key);
        }

        if (compareKey(key, curKey, page->header.attrType) >= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            unsigned childPageNum = *(unsigned*)((char*)page->data + offset + curKeyLength);
            ixfileHandle.readPage(childPageNum, page);
            return findLeafPage(ixfileHandle, page, key);
        }
        offset += curKeyLength + sizeof(unsigned);
    }

    // if key >= lastKey, skip
    char* lastKey = (char*)page + page->header.lastEntryOffset;
    if (compareKey(key, lastKey, page->header.attrType) >= 0) {
        unsigned childPageNum = *(int*)((char*)page + page->header.freeSpaceOffset - sizeof(unsigned));
        ixfileHandle.readPage(childPageNum, page);
        return findLeafPage(ixfileHandle, page, key);
    }

    delete(page);
    return NULL;

}


// try to redistribute 2 pages,
// success: return true, redistributed leftPage and rightPage
// failure: return false, untouched leftPage and rightPage
bool IndexManager::redistribute2Pages(IXPage *leftPage, IXPage *rightPage) {

    int pageCapacity = PAGE_SIZE - sizeof(IXPageHeader);

    char pageType = leftPage->header.pageType;

    // init a buffer named newSpace
    char* newSpace = (char*)malloc(PAGE_SIZE * 2);
    int offsetInNewSpace = 0;

    // append entries in left page to newSpace
    int totalEntryLength = leftPage->header.freeSpaceOffset - sizeof(IXPageHeader);
    memcpy(newSpace + offsetInNewSpace, leftPage->data, totalEntryLength);
    offsetInNewSpace += totalEntryLength;

    // append entries in right page to newSpace
    totalEntryLength = rightPage->header.freeSpaceOffset - sizeof(IXPageHeader);
    memcpy(newSpace + offsetInNewSpace, rightPage->data, totalEntryLength);
    offsetInNewSpace += totalEntryLength;


    int allEntryCount = leftPage->header.entryCount + rightPage->header.entryCount;

    int firstHalfOffset = 0;
    int prevFirstHalfOffset = 0;

    // split newSpace evenly (rough)
    // find the first entry for the second page pointed by firstHalfOffset
    int firstHalfEntryCount = 0;
    for (int i = 0; i < allEntryCount; i++) {
        if (firstHalfOffset >= offsetInNewSpace / 2) {                  // first entry after mid point
            break;
        }
        int curKeyLength = key_length(leftPage->header.attrType, newSpace + firstHalfOffset);

        int curEntryLength = curKeyLength + (pageType == LEAF_PAGE_TYPE ? sizeof(RID) : sizeof(unsigned));
        prevFirstHalfOffset = firstHalfOffset;
        firstHalfOffset += curEntryLength;
        firstHalfEntryCount++;
    }


    // locate the end of the other half and the last entry
    int totalOffset = firstHalfOffset;                  // end
    int prevTotalOffset = firstHalfOffset;              // last entry

    for (int i = 0; i < allEntryCount - firstHalfEntryCount; i++) {
        prevTotalOffset = totalOffset;
        int curKeyLength = key_length(leftPage->header.attrType, newSpace + totalOffset);
        int curEntryLength = curKeyLength + (pageType == LEAF_PAGE_TYPE ? sizeof(RID) : sizeof(unsigned));
        totalOffset += curEntryLength;
    }


    // check if any redistributed page will underflow
    if (firstHalfOffset < pageCapacity / 2 || totalOffset - firstHalfOffset < pageCapacity / 2) {
        free(newSpace);
        return false;
    }


    // the two pages will be valid after redistribution, so begin redistribution
    // copy first half from newSpace to leftPage
    memcpy(leftPage->data, newSpace, firstHalfOffset);
    leftPage->header.entryCount = firstHalfEntryCount;
    leftPage->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
    leftPage->header.freeSpaceSize = pageCapacity - firstHalfOffset;
    leftPage->header.lastEntryOffset = sizeof(IXPageHeader) + prevFirstHalfOffset;

    // copy second half from newSpace to rightPage
    memcpy(rightPage->data, newSpace + firstHalfOffset, totalOffset - firstHalfOffset);
    rightPage->header.entryCount = allEntryCount - firstHalfEntryCount;
    rightPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - firstHalfOffset;
    rightPage->header.freeSpaceSize = pageCapacity - totalOffset + firstHalfOffset;
    rightPage->header.lastEntryOffset = sizeof(IXPageHeader) + prevTotalOffset - firstHalfOffset;

    free(newSpace);
    return true;
}


// deleteOffset is counted from page->data
// assuming this function is validated before using it
void IndexManager::deleteEntryFromPage(IXPage *page, short deleteOffset) {

    int keyLength = key_length(page->header.attrType, page->data + deleteOffset);
    int idLength = page->header.pageType == LEAF_PAGE_TYPE ? sizeof(RID) : sizeof(unsigned);
    int entryLength = keyLength + idLength;

    // entry to be deleted is the last entry, need to find the second last entry offset to update lastEntryOffset
    if (deleteOffset == page->header.lastEntryOffset - sizeof(IXPageHeader)) {
        short offset = 0;
        for (int i = 0; i < page->header.entryCount - 1; i++) {
            int curKeyLength = key_length(page->header.attrType, page->data + offset);
            offset += curKeyLength + idLength;
        }
        page->header.lastEntryOffset = offset;
    }
    else {
        // copy rest part to overwrite delete entry (shift left)
        int restSize = page->header.freeSpaceOffset - sizeof(IXPageHeader) - deleteOffset - entryLength;
        memcpy(page->data + deleteOffset, page->data + deleteOffset + entryLength, restSize);
        page->header.lastEntryOffset -= entryLength;
    }

    page->header.entryCount--;
    page->header.freeSpaceOffset -= entryLength;
    page->header.freeSpaceSize += entryLength;

}


/*bool IndexManager::redistribute2IndexPages(IXFileHandle &ixfileHandle, IXPage *leftPage, IXPage *rightPage, IXPage *parentPage) {
    // redistribute evenly between left and right page through parent;
    // copy all entries in left + entryInParent + entries in right page
    char *newSpace = (char*)malloc(PAGE_SIZE * 2);
    int offsetInNewSpace = 0;

    memcpy(newSpace, leftPage->data, leftPage->header.freeSpaceOffset - sizeof(IXPageHeader));
    offsetInNewSpace += leftPage->header.freeSpaceOffset - sizeof(IXPageHeader);

    // only copy the key in the parent, and copy the leftMostPtr in rightPage
    int rightPageKeyInParentOffset = findEntryOffsetInIndexPage(parentPage, rightPage->header.pageNum);
    int rightPageKeyInParentLength = key_length(parentPage->header.attrType, (char*)parentPage->data + rightPageKeyInParentOffset);

    int rightPageEntryInParentLength = rightPageKeyInParentLength + sizeof(unsigned);


    memcpy((char*)entryInParent, (char*)parentPage->data + rightPageKeyInParentOffset,
           rightPageEntryInParentLength);


    //int keyInParentLength = key_length(page->header.attrType, entryInParent);
    memcpy(newSpace + offsetInNewSpace, entryInParent, rightPageKeyInParentLength);
    offsetInNewSpace += rightPageKeyInParentLength;
    *(unsigned*)(newSpace + offsetInNewSpace) = rightPage->header.leftmostPtr;
    offsetInNewSpace += sizeof(unsigned);

    memcpy(newSpace + offsetInNewSpace, rightPage->data, rightPage->header.freeSpaceOffset - sizeof(IXPageHeader));
    offsetInNewSpace += rightPage->header.freeSpaceOffset - sizeof(IXPageHeader);

    int allEntryCount = leftPage->header.entryCount + rightPage->header.entryCount;

    int firstHalfOffset = 0;
    int prevFirstHalfOffset = 0;

    int halfEntryCount = 0;
    // handel space nicely for varchar key
    for (int i = 0; i < allEntryCount; i++) {
        if (firstHalfOffset > (offsetInNewSpace - rightPageEntryInParentLength) / 2) {
            break;
        }
        prevFirstHalfOffset = firstHalfOffset;
        int curKeyLength = key_length(parentPage->header.attrType, newSpace + firstHalfOffset);
        firstHalfOffset += curKeyLength;

        // adjust children's parentNum
        int childPid = *(int*)(newSpace + firstHalfOffset);
        IXPage *childPage = new IXPage;
        ixfileHandle.readPage(childPid, childPage);
        childPage->header.parent = leftPage->header.pageNum;
        ixfileHandle.writePage(childPid, childPage);
        delete(childPage);
        //-----------------

        halfEntryCount++;
        firstHalfOffset += sizeof(unsigned);
    }



    // modify the entry in parent
    //TODO: what if the modified key in parent is different from previous key length
    memcpy((char*)parentPage->data + rightPageKeyInParentOffset, newSpace + firstHalfOffset, rightPageKeyInParentLength);
    ixfileHandle.writePage(parentPageNum, parentPage);

    // modify the leftMostPtr in rightPage
    rightPage->header.leftmostPtr = *(unsigned*)(newSpace + firstHalfOffset + rightPageKeyInParentLength);
    int rightPageLeftMostPid = rightPage->header.leftmostPtr;
    IXPage *childPage = new IXPage;
    ixfileHandle.readPage(rightPageLeftMostPid, childPage);
    childPage->header.parent = rightPage->header.pageNum;
    ixfileHandle.writePage(rightPageLeftMostPid, childPage);
    delete(childPage);

    int secondHalfBeginOffset = firstHalfOffset + rightPageEntryInParentLength;

    // locate the offsets of the end of other half and the last entry
    int totalOffset = secondHalfBeginOffset;                  // end
    int prevTotalOffset = secondHalfBeginOffset;              // last entry

    // ------------------------------------------------
    //calculate offset of the end and the last entry of the other half except the returned entry
    // handel space nicely for varchar key

    for (int i = 0; i < allEntryCount - halfEntryCount; i++) {
        prevTotalOffset = totalOffset;
        int curKeyLength = key_length(page->header.attrType, newSpace + totalOffset);
        totalOffset += curKeyLength;

        // adjust children's parentNum
        int childPid = *(int*)(newSpace + totalOffset);
        IXPage *childPage = new IXPage;
        ixfileHandle.readPage(childPid, childPage);
        childPage->header.parent = rightPage->header.pageNum;
        ixfileHandle.writePage(childPid, childPage);
        delete(childPage);
        //-----------------

        totalOffset += sizeof(unsigned);
    }

    // copy firsthalf from newspace to leftPage
    memcpy((char*)leftPage + sizeof(IXPageHeader), newSpace, firstHalfOffset);

    // copy secondhalf from newspace to rightPage
    memcpy((char*)rightPage + sizeof(IXPageHeader), newSpace + secondHalfBeginOffset,
           totalOffset - secondHalfBeginOffset);
}*/


int IndexManager::deleteTree(IXFileHandle &ixfileHandle, IXPage *page, const void *key, const RID &rid, void* &oldChildEntry) {

    char pageType = page->header.pageType;
    int freeSpaceCapacity = PAGE_SIZE - sizeof(IXPageHeader);

    //  case 1: if the page is leaf page-----------------------------------------------------
    if (pageType == LEAF_PAGE_TYPE) {

        int keyNum = *(int*)((char*)key);

        // locate the entry to be deleted
        // !!!!!!! deleteOffset doesn't include size of IXPageHeader !!!!!!!!!!!!!!!!!!!!
        int countNode = 0;
        int deleteOffset = findDeleteOffset(page, key, rid, countNode);

        // case 1.1: entry to be deleted not found on this page
        if (deleteOffset == -1) {
            return -1;    // invalid delete entry
        }


        // case 1.2: entry to be deleted is found on this page
        // delete the entry first
        deleteEntryFromPage(page, deleteOffset);

        //TODO:!!!!!!!!!!!!!!!!!!!the deleted entry is the last rid
        // the deleted entry is the last rid
        if (page->header.entryCount == 0 && page->header.nextPageNum == page->header.pageNum &&
            page->header.prevPageNum == 2) {

            IXPage *parentPage = new IXPage;
            ixfileHandle.readPage(page->header.parent, parentPage);
            parentPage->header.entryCount = 0;
            parentPage->header.freeSpaceOffset = sizeof(IXPageHeader);
            parentPage->header.freeSpaceSize = PAGE_SIZE - sizeof(IXPageHeader);
            parentPage->header.lastEntryOffset = sizeof(IXPageHeader);
            ixfileHandle.writePage(parentPage->header.pageNum, parentPage);
            delete(parentPage);

            ixfileHandle.writePage(page->header.pageNum, page);
            if (oldChildEntry) {
                free(oldChildEntry);
            }
            oldChildEntry = NULL;
            return 0;

        }

        IXPage *leftSiblingPage = new IXPage;
        IXPage *rightSiblingPage = new IXPage;

        // try to read out left sibling page, otherwise set it to null
        if (page->header.prevPageNum != page->header.pageNum) {
            ixfileHandle.readPage(page->header.prevPageNum, leftSiblingPage);
            if (leftSiblingPage->header.parent != page->header.parent || leftSiblingPage->header.pageNum == 2) {
                delete(leftSiblingPage);
                leftSiblingPage = NULL;
            }
        } else {
            delete(leftSiblingPage);
            leftSiblingPage = NULL;
        }

        // try to read out right sibling page, otherwise set it to null
        if (page->header.nextPageNum != page->header.pageNum) {
            ixfileHandle.readPage(page->header.nextPageNum, rightSiblingPage);
            if (rightSiblingPage->header.parent != page->header.parent) {
                delete(rightSiblingPage);
                rightSiblingPage = NULL;
            }
        } else {
            delete(rightSiblingPage);
            rightSiblingPage = NULL;
        }

        // case 1.2.1: no merge and redistribution needed
        // if occupancy after deletion is >= 50%, or there's no left and right sibling
        // TODO: validate the condition after ||
        if (page->header.freeSpaceSize < freeSpaceCapacity / 2 ||
            (leftSiblingPage == NULL && rightSiblingPage == NULL)) {
            ixfileHandle.writePage(page->header.pageNum, page);
            if (oldChildEntry) {
                free(oldChildEntry);
            }
            oldChildEntry = NULL;
            //ixfileHandle.writePage(page->header.pageNum, page);
            return 0;
        }

        // case 1.2.2: occupancy less than 50% , have to redistribution or even merge
        else {
            // first, try redistribution !
            bool redistributionSuccess = false;
            bool canMerge = false;                      // check if two pages can be merged
            IXPage *leftPage = NULL;
            IXPage *rightPage = NULL;                   // used to update parent index entry's key

            // try redistribution with leftSibiling
            if (leftSiblingPage != NULL) {
                leftPage = leftSiblingPage;
                rightPage = page;
                redistributionSuccess = redistribute2Pages(leftSiblingPage, page);

                unsigned leftSiblingTotalEntryLength = leftSiblingPage->header.freeSpaceOffset - sizeof(IXPageHeader);
                unsigned pageTotalEntryLength = page->header.freeSpaceOffset - sizeof(IXPageHeader);
                unsigned mergedSize = leftSiblingTotalEntryLength + pageTotalEntryLength;
                canMerge = leftSiblingPage->header.entryCount != 0 && mergedSize <= freeSpaceCapacity;
            }


            // if redistribution with leftSiblingPage fails, try rightSiblingPage
            if (!redistributionSuccess && rightSiblingPage != NULL ) {
                leftPage = page;
                rightPage = rightSiblingPage;
                redistributionSuccess = redistribute2Pages(page, rightSiblingPage);

                unsigned rightSiblingTotalEntryLength = rightSiblingPage->header.freeSpaceOffset - sizeof(IXPageHeader);
                unsigned pageTotalEntryLength = page->header.freeSpaceOffset - sizeof(IXPageHeader);
                unsigned mergedSize = rightSiblingTotalEntryLength + pageTotalEntryLength;
                canMerge = rightSiblingPage->header.entryCount != 0 && mergedSize<= freeSpaceCapacity;
            }

            if (redistributionSuccess) {
                // then need to replace the key of the index entry in parent pointing at the rightPage
                // read parent page out
                IXPage *parentPage = new IXPage;
                ixfileHandle.readPage(rightPage->header.parent, parentPage);

                // find the index entry in parent page that points at right page
                /*
                char* indexEntryPtr = parentPage->data;
                for (short i = 0; i < parentPage->header.entryCount; i++) {
                    int curKeyLength = key_length(rightPage->header.attrType, indexEntryPtr);
                    if (*(unsigned*)(indexEntryPtr + curKeyLength) == rightPage->header.pageNum) {      // found
                        break;
                    }
                    indexEntryPtr += curKeyLength + sizeof(unsigned);
                }*/
                int entryOffsetInParent = findEntryOffsetInIndexPage(parentPage, rightPage->header.pageNum);

                // replace the key value of the target index entry
                // TODO: what if the oldKeyLength < newKeyLength in TypeVarChar case???
                // TODO: need to shift the rest entry, or even split page!!!
                // TODO: use oldKeyLength temporarily to make sure PID is not corrupted
                int oldIndexKeyLength = key_length(rightPage->header.attrType, rightPage->data);

                int keyInParentLength = key_length(parentPage->header.attrType, (char*)parentPage->data + entryOffsetInParent);
                if (keyInParentLength != oldIndexKeyLength) {
                    // shift the data in parent page and leave space for the new key
                    memcpy((char*)parentPage->data + entryOffsetInParent + oldIndexKeyLength,
                           (char*)parentPage->data + entryOffsetInParent + keyInParentLength,
                           parentPage->header.freeSpaceOffset - entryOffsetInParent - keyInParentLength);

                    parentPage->header.freeSpaceOffset += oldIndexKeyLength - keyInParentLength;
                    parentPage->header.freeSpaceSize = PAGE_SIZE - parentPage->header.freeSpaceOffset;

                    if (keyInParentLength != parentPage->header.lastEntryOffset) {
                        parentPage->header.lastEntryOffset += oldIndexKeyLength - keyInParentLength;
                    }
                }


                memcpy((char*)parentPage->data + entryOffsetInParent, rightPage->data, oldIndexKeyLength);

                ixfileHandle.writePage(parentPage->header.pageNum, parentPage);
                delete(parentPage);


                ixfileHandle.writePage(leftPage->header.pageNum, leftPage);
                ixfileHandle.writePage(rightPage->header.pageNum, rightPage);

                if (oldChildEntry) {
                    free(oldChildEntry);
                    oldChildEntry = NULL;
                }

                if (leftSiblingPage) {
                    delete(leftSiblingPage);
                    leftSiblingPage = NULL;
                }
                if (rightSiblingPage) {
                    delete(rightSiblingPage);
                    rightSiblingPage = NULL;
                }
                return 0;
            }


            // TODO:
            canMerge = false;
            int totalEntryOffset = 0;
            if (leftSiblingPage != NULL) {
                totalEntryOffset = leftSiblingPage->header.freeSpaceOffset + page->header.freeSpaceOffset - 2 * sizeof(IXPageHeader);
                if (totalEntryOffset <= freeSpaceCapacity && leftSiblingPage->header.entryCount != 0) {
                    canMerge = true;
                    leftPage = leftSiblingPage;
                    rightPage = page;
                }
            }else if (rightSiblingPage != NULL) {
                totalEntryOffset = rightSiblingPage->header.freeSpaceOffset + page->header.freeSpaceOffset - 2 * sizeof(IXPageHeader);
                if (totalEntryOffset <= freeSpaceCapacity && rightSiblingPage->header.entryCount != 0) {
                    canMerge = true;
                    leftPage = page;
                    rightPage = rightSiblingPage;
                }
            }
            // redistribution with left and right both failed

            // rare cases where merge is impossible, leave underflow page as it is
            if (!canMerge) {
                ixfileHandle.writePage(page->header.pageNum, page);
                if (oldChildEntry) {
                    free(oldChildEntry);
                    oldChildEntry = NULL;
                }
                if (leftSiblingPage) {
                    delete(leftSiblingPage);
                    leftSiblingPage = NULL;
                }
                if (rightSiblingPage) {
                    delete(rightSiblingPage);
                    rightSiblingPage = NULL;
                }
                return 0;
            }

            // need to merge in leaf page-------------------------------------------------------------------------------

            // oldchildentry = & (current entry in parent for right);
            /*int keyInParentLength = key_length(page->header.attrType, rightPage->data);
            void *keyInParent = malloc(keyInParentLength);
            memcpy((char*)keyInParent, rightPage->data, keyInParentLength);*/

            int parentPageNum = rightPage->header.parent;
            IXPage *parentPage = new IXPage;
            ixfileHandle.readPage(parentPageNum, parentPage);

            int keyOffsetInParent = findEntryOffsetInIndexPage(parentPage, rightPage->header.pageNum);
            int entryInParentLength = key_length(page->header.attrType,
                                                 (char*)parentPage->data + keyOffsetInParent) + sizeof(unsigned);

            void* entryInParent = malloc(entryInParentLength);
            memcpy((char*)entryInParent, (char*)parentPage->data + keyOffsetInParent, entryInParentLength);

            oldChildEntry = entryInParent;
            int oldChildPid = *(int*)((char*)oldChildEntry + entryInParentLength - sizeof(unsigned));

            //free(keyInParent);
            delete(parentPage);
            //free(entryInParent);

            // move entries from right to left, discard right page ????????
            memcpy((char*)leftPage + leftPage->header.freeSpaceOffset, (char*)rightPage->data,
                   rightPage->header.freeSpaceOffset - sizeof(IXPageHeader));

            //adjust sibling pointers
            if (rightPage->header.nextPageNum == rightPage->header.pageNum) {

                leftPage->header.nextPageNum = leftPage->header.pageNum;
            } else {
                leftPage->header.nextPageNum = rightPage->header.nextPageNum;

                IXPage *pageNextToRightPage = new IXPage;
                ixfileHandle.readPage(rightPage->header.nextPageNum, pageNextToRightPage);
                pageNextToRightPage->header.prevPageNum = leftPage->header.pageNum;
                ixfileHandle.writePage(pageNextToRightPage->header.pageNum, pageNextToRightPage);
                delete(pageNextToRightPage);
            }

            // adjust leftPage header offspace, entryCount, lastEntryOffset
            leftPage->header.entryCount += rightPage->header.entryCount;
            leftPage->header.freeSpaceOffset += rightPage->header.freeSpaceOffset - sizeof(IXPageHeader);
            leftPage->header.freeSpaceSize = PAGE_SIZE - leftPage->header.freeSpaceOffset;
            leftPage->header.lastEntryOffset = leftPage->header.freeSpaceOffset
                    - (rightPage->header.freeSpaceOffset - rightPage->header.lastEntryOffset);

            ixfileHandle.writePage(leftPage->header.pageNum, leftPage);
            ixfileHandle.writePage(rightPage->header.pageNum, rightPage);

            if (leftSiblingPage) {
                delete(leftSiblingPage);
                leftSiblingPage = NULL;
            }
            if (rightSiblingPage) {
                delete(rightSiblingPage);
                rightSiblingPage = NULL;
            }

            return 0;
        }

    }



    // case 2: page is non-leaf page------------------------------------------------------------------------------------
    else {
        IXPage *nextPage = findNextPage(ixfileHandle, page, key);

        if (deleteTree(ixfileHandle, nextPage, key, rid, oldChildEntry) == -1) {
            return -1;
        } //recursively delete entry in the leaf page

        // case 2.1: usual case, child is not merge or redistribution, done
        if (oldChildEntry == NULL) {
            delete(nextPage);
            return 0;
        }

        // case 2.2: child page has been discard

        //find delete offset in index page, the param is entry not key, has to compare childPid
        int deleteOffsetInIndexPage = findDeleteOffsetInIndexPage(page, oldChildEntry);
        int oldChildKeyLength = key_length(page->header.attrType, (char*)page->data + deleteOffsetInIndexPage);
        int oldChildEntryLength = oldChildKeyLength + sizeof(unsigned);

        int oldChildPid = *(int*)((char*)oldChildEntry + oldChildKeyLength);

        // delete oldChildEntry
        int restSize = page->header.freeSpaceOffset - sizeof(IXPageHeader) - deleteOffsetInIndexPage - oldChildEntryLength;
        memcpy((char*)page->data + deleteOffsetInIndexPage,
               (char*)page->data + deleteOffsetInIndexPage + oldChildEntryLength, restSize);

        // adjust entryCount, freespaceOffset, freespace size
        page->header.entryCount--;
        page->header.freeSpaceOffset -= oldChildEntryLength;
        page->header.freeSpaceSize += oldChildEntryLength;

        //adjust lastEntryOffset
        if (deleteOffsetInIndexPage != page->header.lastEntryOffset) {
            page->header.lastEntryOffset -= oldChildEntryLength;

            // the deleted entry is the last entry, has to find the new last entry offset
        }else {
            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < page->header.entryCount - 1; i++) {

                int curKeyLength = key_length(page->header.attrType, (char*)page + offset);
                offset += curKeyLength + sizeof(unsigned);
            }
            page->header.lastEntryOffset = offset;
        }

        ixfileHandle.writePage(page->header.pageNum, page);

        // case 2.2.1 : have spared entries, or the index page is root, no merge or redistribution------------------------
        if (page->header.freeSpaceOffset - sizeof(IXPageHeader) > freeSpaceCapacity / 2 || page->header.isRoot) {

            // decrease tree height
            if (page->header.isRoot && page->header.entryCount == 0) {
                IXPage *dirPage = new IXPage;
                ixfileHandle.readPage(0, dirPage);
                dirPage->header.leftmostPtr = page->header.leftmostPtr;
                ixfileHandle.writePage(0, dirPage);

                IXPage *newRootPage = new IXPage;
                ixfileHandle.readPage(dirPage->header.leftmostPtr, newRootPage);
                newRootPage->header.isRoot = true;
                newRootPage->header.parent = newRootPage->header.pageNum;
                ixfileHandle.writePage(dirPage->header.leftmostPtr, newRootPage);

                delete(dirPage);
                delete(newRootPage);
            }

            ixfileHandle.writePage(page->header.pageNum, page);

            // set oldChildEntry to null, stop delete;
            free(oldChildEntry);
            oldChildEntry = NULL;
            delete(nextPage);
            return 0;


        }
        // case 2.2.2 : have no spared entries, merge or redistribution, oldChildEntry has been deleted-----------------
        else {

            IXPage *leftSiblingPage = new IXPage;
            IXPage *rightSiblingPage = new IXPage;

            IXPage *leftPage = NULL;
            IXPage *rightPage = NULL;

            int parentPageNum = page->header.parent;
            IXPage *parentPage = new IXPage;
            ixfileHandle.readPage(parentPageNum, parentPage);

            // find sibling by parent
            int keyOffsetInParent = findEntryOffsetInIndexPage(parentPage, page->header.pageNum);
            int keyInParentLength = key_length(page->header.attrType, (char*)parentPage->data + keyOffsetInParent);

            /*void *keyInParent = malloc(keyInParentLength);
            memcpy(keyInParent, parentPage->data, keyInParentLength);*/

            int entryInParentLength = keyInParentLength + sizeof(unsigned);

            //compose entry in parent(used for redistribution)
            void *entryInParent = malloc(entryInParentLength);
            memcpy((char*)entryInParent, (char*)parentPage->data + keyOffsetInParent, entryInParentLength);


            int leftSiblingPageNum = 0;
            int rightSiblingPageNum = 0;

            if (parentPage->header.leftmostPtr == page->header.pageNum) {  // key is the first key in parent
                leftSiblingPageNum = 0;
                if (parentPage->header.entryCount >= 1) {
                    rightSiblingPageNum = *(unsigned*)((char*)parentPage->data + keyInParentLength);
                }

            }else if (keyOffsetInParent + sizeof(IXPageHeader) == parentPage->header.lastEntryOffset) { // key is the last key in parent
                if (parentPage->header.entryCount == 1) {
                    leftSiblingPageNum = parentPage->header.leftmostPtr;
                }else {
                    leftSiblingPageNum = *(unsigned *) ((char *) parentPage->data + keyOffsetInParent -
                                                        sizeof(unsigned));
                }
                rightSiblingPageNum = 0;

            }else if (keyOffsetInParent == 0) {

                leftSiblingPageNum = parentPage->header.leftmostPtr;

                int nextKeyLength = key_length(page->header.attrType,
                                               (char*)parentPage->data + keyOffsetInParent + entryInParentLength);
                int offsetTemp = keyOffsetInParent + entryInParentLength + nextKeyLength;

                rightSiblingPageNum = *(unsigned*)((char*)parentPage->data + offsetTemp);

            } else {
                leftSiblingPageNum = *(unsigned*)((char*)parentPage->data + keyOffsetInParent - sizeof(unsigned));

                int nextKeyLength = key_length(page->header.attrType,
                                               (char*)parentPage->data + keyOffsetInParent + entryInParentLength);
                int offsetTemp = keyOffsetInParent + entryInParentLength + nextKeyLength;

                rightSiblingPageNum = *(unsigned*)((char*)parentPage->data + offsetTemp);
            }



            if (leftSiblingPageNum != 0) {
                ixfileHandle.readPage(leftSiblingPageNum, leftSiblingPage);
            }else if (leftSiblingPage != NULL){
                delete(leftSiblingPage);
                leftSiblingPage = NULL;
            }
            if (rightSiblingPageNum != 0) {
                ixfileHandle.readPage(rightSiblingPageNum, rightSiblingPage);
            }else if (rightSiblingPage != NULL){
                delete(rightSiblingPage);
                rightSiblingPage = NULL;
            }

            // leftSiblingPage has spare entries, set leftpage point to leftsibling page, rightPage point to page
            if (leftSiblingPage != NULL && leftSiblingPage->header.freeSpaceSize < freeSpaceCapacity / 2) {
                leftPage = leftSiblingPage;
                rightPage = page;

                // rightSiblingPage has spare entries,
            }else if (rightSiblingPage != NULL && rightSiblingPage->header.freeSpaceSize < freeSpaceCapacity / 2) {
                leftPage = page;
                rightPage = rightSiblingPage;
            }

            // ------------redistribution of index page------------------------
            if (leftPage != NULL && rightPage != NULL) {

                // redistribute evenly between left and right page through parent;
                // copy all entries in left + entryInParent + entries in right page
                char *newSpace = (char*)malloc(PAGE_SIZE * 2);
                int offsetInNewSpace = 0;

                memcpy(newSpace, leftPage->data, leftPage->header.freeSpaceOffset - sizeof(IXPageHeader));
                offsetInNewSpace += leftPage->header.freeSpaceOffset - sizeof(IXPageHeader);

                // only copy the key in the parent, and copy the leftMostPtr in rightPage
                int rightPageKeyInParentOffset = findEntryOffsetInIndexPage(parentPage, rightPage->header.pageNum);
                int rightPageKeyInParentLength = key_length(page->header.attrType, (char*)parentPage->data + rightPageKeyInParentOffset);

                int rightPageEntryInParentLength = rightPageKeyInParentLength + sizeof(unsigned);


                memcpy((char*)entryInParent, (char*)parentPage->data + rightPageKeyInParentOffset,
                       rightPageEntryInParentLength);


                //int keyInParentLength = key_length(page->header.attrType, entryInParent);
                memcpy(newSpace + offsetInNewSpace, entryInParent, rightPageKeyInParentLength);
                offsetInNewSpace += rightPageKeyInParentLength;
                *(unsigned*)(newSpace + offsetInNewSpace) = rightPage->header.leftmostPtr;
                offsetInNewSpace += sizeof(unsigned);

                memcpy(newSpace + offsetInNewSpace, rightPage->data, rightPage->header.freeSpaceOffset - sizeof(IXPageHeader));
                offsetInNewSpace += rightPage->header.freeSpaceOffset - sizeof(IXPageHeader);

                int allEntryCount = leftPage->header.entryCount + rightPage->header.entryCount;

                int firstHalfOffset = 0;
                int prevFirstHalfOffset = 0;

                int halfEntryCount = 0;
                // handel space nicely for varchar key
                for (int i = 0; i < allEntryCount; i++) {
                    if (firstHalfOffset > (offsetInNewSpace - rightPageEntryInParentLength) / 2) {
                        break;
                    }
                    prevFirstHalfOffset = firstHalfOffset;
                    int curKeyLength = key_length(page->header.attrType, newSpace + firstHalfOffset);
                    firstHalfOffset += curKeyLength;

                    // adjust children's parentNum
                    int childPid = *(int*)(newSpace + firstHalfOffset);
                    IXPage *childPage = new IXPage;
                    ixfileHandle.readPage(childPid, childPage);
                    childPage->header.parent = leftPage->header.pageNum;
                    ixfileHandle.writePage(childPid, childPage);
                    delete(childPage);
                    //-----------------

                    halfEntryCount++;
                    firstHalfOffset += sizeof(unsigned);
                }



                // modify the entry in parent
                //TODO: what if the modified key in parent is different from previous key length
                int newKeyInParentLength = key_length(parentPage->header.attrType, newSpace + firstHalfOffset);
                if (newKeyInParentLength != rightPageKeyInParentLength) {
                    // shift the data in parent page and leave space for the new key
                    memcpy((char*)parentPage->data + rightPageKeyInParentOffset + newKeyInParentLength,
                           (char*)parentPage->data + rightPageKeyInParentOffset + rightPageKeyInParentLength,
                           parentPage->header.freeSpaceOffset - rightPageKeyInParentOffset - rightPageKeyInParentLength);

                    parentPage->header.freeSpaceOffset += newKeyInParentLength - rightPageKeyInParentLength;
                    parentPage->header.freeSpaceSize = PAGE_SIZE - parentPage->header.freeSpaceOffset;

                    if (rightPageKeyInParentOffset != parentPage->header.lastEntryOffset) {
                        parentPage->header.lastEntryOffset += newKeyInParentLength - rightPageKeyInParentLength;
                    }
                }
                memcpy((char*)parentPage->data + rightPageKeyInParentOffset, newSpace + firstHalfOffset, rightPageKeyInParentLength);
                ixfileHandle.writePage(parentPageNum, parentPage);

                // modify the leftMostPtr in rightPage
                rightPage->header.leftmostPtr = *(unsigned*)(newSpace + firstHalfOffset + rightPageKeyInParentLength);
                int rightPageLeftMostPid = rightPage->header.leftmostPtr;
                IXPage *childPage = new IXPage;
                ixfileHandle.readPage(rightPageLeftMostPid, childPage);
                childPage->header.parent = rightPage->header.pageNum;
                ixfileHandle.writePage(rightPageLeftMostPid, childPage);
                delete(childPage);

                int secondHalfBeginOffset = firstHalfOffset + rightPageEntryInParentLength;

                // locate the offsets of the end of other half and the last entry
                int totalOffset = secondHalfBeginOffset;                  // end
                int prevTotalOffset = secondHalfBeginOffset;              // last entry

                // ------------------------------------------------
                //calculate offset of the end and the last entry of the other half except the returned entry
                // handel space nicely for varchar key

                for (int i = 0; i < allEntryCount - halfEntryCount; i++) {
                    prevTotalOffset = totalOffset;
                    int curKeyLength = key_length(page->header.attrType, newSpace + totalOffset);
                    totalOffset += curKeyLength;

                    // adjust children's parentNum
                    int childPid = *(int*)(newSpace + totalOffset);
                    IXPage *childPage = new IXPage;
                    ixfileHandle.readPage(childPid, childPage);
                    childPage->header.parent = rightPage->header.pageNum;
                    ixfileHandle.writePage(childPid, childPage);
                    delete(childPage);
                    //-----------------

                    totalOffset += sizeof(unsigned);
                }

                // copy firsthalf from newspace to leftPage
                memcpy((char*)leftPage + sizeof(IXPageHeader), newSpace, firstHalfOffset);

                // copy secondhalf from newspace to rightPage
                memcpy((char*)rightPage + sizeof(IXPageHeader), newSpace + secondHalfBeginOffset,
                       totalOffset - secondHalfBeginOffset);

                // adjust header in leftPage and rightPage
                leftPage->header.entryCount = halfEntryCount;
                leftPage->header.freeSpaceOffset = sizeof(IXPageHeader) + firstHalfOffset;
                leftPage->header.freeSpaceSize = PAGE_SIZE - sizeof(IXPageHeader) - firstHalfOffset;
                leftPage->header.lastEntryOffset = sizeof(IXPageHeader) + prevFirstHalfOffset;

                rightPage->header.entryCount = allEntryCount - halfEntryCount;
                rightPage->header.freeSpaceOffset = sizeof(IXPageHeader) + totalOffset - secondHalfBeginOffset;
                rightPage->header.freeSpaceSize = PAGE_SIZE - rightPage->header.freeSpaceOffset;
                rightPage->header.lastEntryOffset = sizeof(IXPageHeader) + prevTotalOffset - secondHalfBeginOffset;

                ixfileHandle.writePage(leftPage->header.pageNum, leftPage);
                ixfileHandle.writePage(rightPage->header.pageNum, rightPage);

                free(oldChildEntry);
                oldChildEntry = NULL;
                free(newSpace);

                //free(keyInParent);
                free(entryInParent);


            }
            //  ---------------------merge index page, oldChildEntry has been deleted ----------------------------------
            else {
                bool canMerge = false;
                int totalEntryOffset = 0;
                if (leftSiblingPage != NULL) {
                    totalEntryOffset = leftSiblingPage->header.freeSpaceOffset + page->header.freeSpaceOffset - 2 * sizeof(IXPageHeader);
                    if (totalEntryOffset <= freeSpaceCapacity) {
                        canMerge = true;
                        leftPage = leftSiblingPage;
                        rightPage = page;
                    }
                } else if (rightSiblingPage != NULL) {
                    totalEntryOffset = rightSiblingPage->header.freeSpaceOffset + page->header.freeSpaceOffset - 2 * sizeof(IXPageHeader);
                    if (totalEntryOffset <= freeSpaceCapacity) {
                        canMerge = true;
                        leftPage = page;
                        rightPage = rightSiblingPage;
                    }
                }
                if (!canMerge) {
                    free(oldChildEntry);
                    oldChildEntry = NULL;
                    return 0;
                }

                // find entry in parent of leftpage
                //int rightPageKeyInParentLength = key_length(page->header.attrType, rightPage->data);
                /*void *leftPageKeyInParent = malloc(leftPageKeyInParentLength);
                memcpy((char*)leftPageKeyInParent, (char*)leftPage->data, leftPageKeyInParentLength);*/


                int rightPageKeyInParentOffset = findEntryOffsetInIndexPage(parentPage, rightPage->header.pageNum);

                // oldchildentry = & (current entry in parent for M);
                int rightPageKeyInParentLength = key_length(page->header.attrType, (char*)parentPage->data + rightPageKeyInParentOffset);

                int rightPageEntryInParentLength = rightPageKeyInParentLength + sizeof(unsigned);
                void *rightPageEntryInParent = malloc(rightPageEntryInParentLength);

                memcpy((char*)rightPageEntryInParent, (char*)parentPage->data + rightPageKeyInParentOffset,
                       rightPageEntryInParentLength);

                oldChildEntry = rightPageEntryInParent;


                // adjust the children page's in rightPage parentNum to leftPage
                IXPage *childPage = new IXPage;
                ixfileHandle.readPage(rightPage->header.leftmostPtr, childPage);
                childPage->header.parent = leftPage->header.pageNum;
                ixfileHandle.writePage(rightPage->header.leftmostPtr, childPage);
                delete(childPage);

                int offsetTemp2 = sizeof(IXPageHeader);
                for (int i = 0; i < rightPage->header.entryCount; i++) {
                    int curKeyLength = key_length(page->header.attrType, (char*)rightPage + offsetTemp2);
                    offsetTemp2 += curKeyLength;

                    // adjust children's parentNum
                    int childPid = *(int*)((char*)rightPage + offsetTemp2);
                    IXPage *childPage = new IXPage;
                    ixfileHandle.readPage(childPid, childPage);
                    childPage->header.parent = leftPage->header.pageNum;
                    ixfileHandle.writePage(childPid, childPage);
                    delete(childPage);
                    //-----------------

                    offsetTemp2 += sizeof(unsigned);
                }
                //-----------------


                // copy key in leftPageEntryInParent + rightpage to leftpage, copy the leftMostPtr in rightPage
                // and discard right page?????
                memcpy((char*)leftPage + leftPage->header.freeSpaceOffset, (char*)rightPageEntryInParent,
                       rightPageKeyInParentLength);

                int offsetTemp = leftPage->header.freeSpaceOffset + rightPageKeyInParentLength;

                *(unsigned*)((char*)leftPage + offsetTemp) = rightPage->header.leftmostPtr;
                offsetTemp += sizeof(unsigned);

                memcpy((char*)leftPage + offsetTemp, (char*)rightPage->data,
                       rightPage->header.freeSpaceOffset - sizeof(IXPageHeader));

                // adjust leftPage header offspace, entryCount, lastEntryOffset
                leftPage->header.entryCount += rightPage->header.entryCount + 1;
                leftPage->header.freeSpaceOffset += rightPage->header.freeSpaceOffset - sizeof(IXPageHeader) +
                        rightPageEntryInParentLength;
                leftPage->header.freeSpaceSize = PAGE_SIZE - leftPage->header.freeSpaceOffset;
                leftPage->header.lastEntryOffset = leftPage->header.freeSpaceOffset -
                        (rightPage->header.freeSpaceOffset - rightPage->header.lastEntryOffset);

                ixfileHandle.writePage(leftPage->header.pageNum, leftPage);
                ixfileHandle.writePage(rightPage->header.pageNum, rightPage);

                //free(leftPageKeyInParent);
            }

            //free(keyInParent);
            //free(entryInParent);
            if (leftSiblingPage != NULL) {
                delete (leftSiblingPage);
            }
            if (rightSiblingPage != NULL) {
                delete (rightSiblingPage);
            }
            delete(parentPage);
        }

        delete nextPage;
    }

    return 0;
}

// find delete offset in index page
int IndexManager::findDeleteOffsetInIndexPage(IXPage *page, const void *entry) {

    if (page->header.entryCount == 0) {
        return -1;
    }

    int keyLength = key_length(page->header.attrType, entry);
    unsigned childPid = *(unsigned*)((char*)entry + keyLength);

    int entryCount = page->header.entryCount;
    void *firstKey = (char*)page->data;
    void *lastKey = (char*)page + page->header.lastEntryOffset;

    //key < firstKey, invalid delete
    if (compareKey(entry, firstKey, page->header.attrType) < 0) {
        return -1;

    }else if (compareKey(entry, firstKey, page->header.attrType) == 0) {
        int firstKeyLength = key_length(page->header.attrType, firstKey);

        unsigned firstPid = *(unsigned *)((char*)firstKey + firstKeyLength);
        if (firstPid == childPid) {
            return 0;
        }
    }

    //key > lastKey, invalid delete
    if (compareKey(entry, lastKey, page->header.attrType) > 0) {
        return -1;

    }else if (compareKey(entry, lastKey, page->header.attrType) == 0) {
        int lastKeyLength = key_length(page->header.attrType, lastKey);

        unsigned lastPid = *(unsigned*)((char*)lastKey + lastKeyLength);

        if (lastPid == childPid) {
            return page->header.lastEntryOffset - sizeof(IXPageHeader);
        }
    }

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount; i++) {
        void *curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);

        int compareRes = compareKey(entry, curKey, page->header.attrType);

        if (compareRes == 0) {

            unsigned curPid = *(unsigned*)((char*)page + offset + curKeyLength);

            if (curPid == childPid) {
                return offset - sizeof(IXPageHeader);
            }
        }else if (compareRes < 0) {
            return -1;
        }
        offset += curKeyLength + sizeof(unsigned);
    }

    return -1;
}

// find delete offset in leaf page
int IndexManager::findDeleteOffset(IXPage *page, const void *key, const RID &rid, int &countNode) {
    if (page->header.entryCount == 0) {
        return -1;
    }

    int entryCount = page->header.entryCount;
    void *firstKey = (char*)page->data;
    void *lastKey = (char*)page + page->header.lastEntryOffset;

    //key < firstKey, invalid delete
    if (compareKey(key, firstKey, page->header.attrType) < 0) {
        countNode = 0;
        return -1;

    }else if (compareKey(key, firstKey, page->header.attrType) == 0) {
        int firstKeyLength = key_length(page->header.attrType, firstKey);

        RID firstKeyRid = *(RID*)((char*)firstKey + firstKeyLength);

        if (firstKeyRid.pageNum == rid.pageNum && firstKeyRid.slotNum == rid.slotNum) {
            return 0;
        }
    }

    //key > lastKey, invalid delete
    if (compareKey(key, lastKey, page->header.attrType) > 0) {
        countNode = 0;
        return -1;

    }else if (compareKey(key, lastKey, page->header.attrType) == 0) {
        int lastKeyLength = key_length(page->header.attrType, lastKey);

        RID lastKeyRid = *(RID*)((char*)lastKey + lastKeyLength);

        if (lastKeyRid.pageNum == rid.pageNum && lastKeyRid.slotNum == rid.slotNum) {
            return page->header.lastEntryOffset - sizeof(IXPageHeader);

        }
    }

    int offset = sizeof(IXPageHeader);

    for (int i = 0; i < entryCount; i++) {
        void *curKey = (char*)page + offset;
        int curKeyLength = key_length(page->header.attrType, curKey);

        int compareRes = compareKey(key, curKey, page->header.attrType);

        if (compareRes == 0) {

            RID curKeyRid = *(RID*)((char*)page + offset + curKeyLength);

            if (curKeyRid.pageNum == rid.pageNum && curKeyRid.slotNum == rid.slotNum) {
                return offset - sizeof(IXPageHeader);

            }
        }else if (compareRes < 0) {
            return -1;
        }
        countNode++;
        offset += curKeyLength + sizeof(RID);
    }

    return -1;
}

// find entry offset in parent for sibling page
//  !!!!!!!!!!!!don't include the size of IXPageHeader !!!!!!!!!!!!!!!
int IndexManager::findEntryOffsetInIndexPage(IXPage *page, unsigned childPid) {

    int entryCount = page->header.entryCount;
    int offset = sizeof(IXPageHeader);

    if (page->header.leftmostPtr == childPid) {
        return 0;
    }

    for (short i = 0; i < entryCount; i++) {
        int curKeyLength = key_length(page->header.attrType, (char*)page + offset);
        if (*(unsigned*)((char*)page + offset + curKeyLength) == childPid) {      // found
            return offset - sizeof(IXPageHeader);
        }
        offset += curKeyLength + sizeof(unsigned);
    }
    return -1;

    /*int entryCount = page->header.entryCount;
    int offset = sizeof(IXPageHeader);

    void *firstKey = (char*)page + offset;
    if (compareKey(key, firstKey, page->header.attrType) < 0) {
        return 0;
    }

    void *lastKey = (char*)page + page->header.lastEntryOffset;
    if (compareKey(lastKey, key, page->header.attrType) <= 0) {
        return page->header.lastEntryOffset - sizeof(IXPageHeader);
    }

    for (int i = 0; i < entryCount - 1; i++) {

        int curKeyLength = key_length(page->header.attrType, (char*)page + offset);

        void *curKey = (char*)page + offset;
        void *nextKey = (char*)page + offset + curKeyLength + sizeof(unsigned);

        if (compareKey(curKey, key, page->header.attrType) <= 0 && compareKey(key, nextKey, page->header.attrType) < 0) {
            return offset - sizeof(IXPageHeader);
        }
        offset += curKeyLength + sizeof(unsigned);
    }

    return -1;*/
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

            int leftMostPid = page->header.leftmostPtr;
            pids.push_back(leftMostPid);

            //int test = 0;

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
            cout<<"(pageNum :"<<page->header.pageNum<<")";
            for (int i = 0; i < keys.size(); i++) {
                cout << "\"" << keys[i] << "\"";
                if (i != keys.size() - 1) {
                    cout << ",";
                }
            }
            cout << "]," << endl;
            cout << "\"children\": [" << endl;

            for (int i = 0; i < pids.size(); i++) {
                IXPage *nextPage = new IXPage;
                ixfileHandle.readPage(pids[i], nextPage);
                int nextEntryCount = nextPage->header.entryCount;
                delete(nextPage);

                if (nextEntryCount != 0) {
                    DFSPrintBTree(pids[i], ixfileHandle, attribute);
                    if (i != pids.size() - 1) {
                        cout << "]},"<< endl;
                    }

                }

            }
            cout << "]}"<< endl;
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
            cout << "(pageNum :" << page->header.pageNum << ")";

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

            int leftMostPid = page->header.leftmostPtr;
            pids.push_back(leftMostPid);

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
                IXPage *nextPage = new IXPage;
                ixfileHandle.readPage(pids[i], nextPage);
                int nextEntryCount = nextPage->header.entryCount;
                delete(nextPage);

                if (nextEntryCount != 0) {
                    DFSPrintBTree(pids[i], ixfileHandle, attribute);
                    if (i != pids.size() - 1) {
                        cout << "]},"<< endl;
                    }

                }

            }
            cout << "]}"<< endl;
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

            int leftMostPid = page->header.leftmostPtr;
            pids.push_back(leftMostPid);

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                keyLength = *(int*)((char*)page + offset);
                offset += sizeof(int);

                memcpy(key, (char*)page + offset, keyLength);
                key[keyLength] = '\0';
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
                IXPage *nextPage = new IXPage;
                ixfileHandle.readPage(pids[i], nextPage);
                int nextEntryCount = nextPage->header.entryCount;
                delete(nextPage);

                if (nextEntryCount != 0) {
                    DFSPrintBTree(pids[i], ixfileHandle, attribute);
                    if (i != pids.size() - 1) {
                        cout << "]},"<< endl;
                    }
                }
            }
            cout << "]}"<< endl;
            // is leaf page
        }else {
            vector<string> keys;
            vector<RID> rids;

            int offset = sizeof(IXPageHeader);
            for (int i = 0; i < entryCount; i++) {
                keyLength = *(int *) ((char *) page + offset);
                offset += sizeof(int);

                memcpy(key, (char *) page + offset, keyLength);
                key[keyLength] = '\0';
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

