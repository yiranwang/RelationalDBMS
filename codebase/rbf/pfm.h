#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define DEBUG 0
#define PAGE_SIZE 400//4096
#define HEADER_SIZE 96
#define DATA_SIZE 4000


#include <string>
#include <climits>
using namespace std;

// ========= Start of self defined structures ==========
typedef struct {
    unsigned pageNumber;        // page number
    short recordCount;          // number of records on this page 
    short slotCount;            // number of slots on this page
    short freeSpace;            // size of free space
    short freeSpaceOffset;      // starting address of free space
    char misc[84];
} PageHeader;

typedef struct {
    /*
     *  header(tentative fields):
     *      (short)number of records
     *      (short)free space size
     *  data:
     *      rows of records (grows downwards)
     *      free space
     *      (short)offsets (grows backwards)
     */
    PageHeader header;
    char data[DATA_SIZE];
} Page;

typedef struct
{
    short offset;           // -1 for deleted records, tombstone
    short length;           // -1 for redirected records, whose rid can be found by slot.offset
    unsigned char isRedirected;      // if this record is redirected from somewhere else, it's set to non-zero valuei; once marked, it's marked forever
} Slot;

//  structure of records:
//  short   short       short       ...     
//  #field  f1_offset   f2_offset   ...     field1  field2  field3
//  if offset == -1, this value is NULL

// =========== End of self defined structures ========

class FileHandle;

class PagedFileManager {
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file

protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
};

class FileHandle {
public:
    // variables to keep the counter for each operation
    char* fileName;
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    int fd;                                                               // file descriptor
    unsigned long totalPages;

    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    // self defined methods
    RC readPageHeader(PageNum pageNum, void *data);
}; 

#endif
