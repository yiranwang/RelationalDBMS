#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// page type bit masks
# define DIR_PAGE_TYPE (0)
# define LEAF_PAGE_TYPE (1)
# define INDEX_PAGE_TYPE (2)
# define ROOT_PAGE_TYPE (4)


typedef struct {

    unsigned pageNum;
    unsigned leftmostPtr;           // leftmost pointer; when this is a directory page, it's used to point to root page
    unsigned prevPageNum;           // double linked list only used in leaf page
    unsigned nextPageNum;

    short freeSpaceSize;            // size of free space
    short freeSpaceOffset;          // starting address of free space

    short entryCount;               // number of entries on this page
    char pageType;                  // indicate if this is a root page, index page or leaf page
    char attrType;                  // attribute type inside this index file: int, float or varChar


    //short firstEntryOffset;       // fixed: sizeof(IXPageHeader)
    short lastEntryOffset;          // can help skip scanning the whole page

} IXPageHeader;


typedef struct {
    IXPageHeader header;
    char data[PAGE_SIZE - sizeof(IXPageHeader)];
} IXPage;


class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;



        // auxiliary functions
        void initializeIndex(IXFileHandle &ixfileHandle, const int attrType);
        IXPage initializeIXPage(unsigned pageNum, char pageType, AttrType attrType);
        IXPage *findLeafPage(IXFileHandle &ixfileHandle, const void *key, const RID &rid);




    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    FileHandle fileHandle;

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);


    RC readPage(PageNum pageNum, void *data) { return fileHandle.readPage(pageNum, data); }
    RC writePage(PageNum pageNum, const void *data) { return fileHandle.writePage(pageNum, data); }
    RC appendPage(const void *data) { return fileHandle.appendPage(data); }

};

#endif
