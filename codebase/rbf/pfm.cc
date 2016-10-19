#include "pfm.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    //If O_CREAT and O_EXCL are set, open() will fail if the file exists
    int fd = open(fileName.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        if(DEBUG) perror("Error in createFile");
        return -1;
    }        
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    int rc = unlink(fileName.c_str());
    return rc;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{ 
    if (fileHandle.fd > 0) {
        printf("file descriptor is larger than 0!\n");
        return -1;
    }

    int fd = open(fileName.c_str(), O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        return -1;
    } else {
        fileHandle.fd = fd;
        return 0;
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    int rc = close(fileHandle.fd);
    fileHandle.fd = -1;
    return rc;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fd = -1;
    totalPages = -1;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (fd < 0) {
        if(DEBUG) fprintf(stderr, "File not open!");
        return -1;
    }
    unsigned totalPages = getNumberOfPages();
    if (pageNum >= totalPages) {
        if(DEBUG) fprintf(stderr, "Error in readPage: pageNum: %d >= total # of pages: %d.\n", pageNum, totalPages);
        return -1;
    }
    if (lseek(fd, pageNum * PAGE_SIZE, SEEK_SET) < 0) {
        if(DEBUG) fprintf(stderr, "Error in readPage: failed to locate the page!\n");
        return -1;
    }
    off_t bytesRead = read(fd, data, PAGE_SIZE);
    if (bytesRead == (off_t) -1) {
        
        if(DEBUG) {
            perror("error in readPage");
            fprintf(stderr, "Error in readPage: failed to read file!\n");
        }
        return -1;
    }
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (fd < 0) {
        if(DEBUG) fprintf(stderr, "File not open!");
        return -1;
    }
    unsigned totalPages = getNumberOfPages();
    if (pageNum >= totalPages) {
        if(DEBUG) fprintf(stderr, "Error in writePage: pageNum: %d >= total # of pages: %d.\n", pageNum, totalPages);
        return -1;
    }
    lseek(fd, pageNum * PAGE_SIZE, SEEK_SET);
    if (write(fd, data, PAGE_SIZE) < 0) {
        if(DEBUG) fprintf(stderr, "Error in writePage: write failed!\n");
    }
    writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if (fd < 0) {
        if(DEBUG) fprintf(stderr, "File not open!");
        return -1;
    }
    // pageNum before appending
    unsigned curTotalPages = getNumberOfPages();
    lseek(fd, 0, SEEK_END);                                            // place position at the end
    if (write(fd, data, PAGE_SIZE) < 0) {
        if(DEBUG) fprintf(stderr, "Error in appendPage: write failed!\n");
        return -1;
    }
    appendPageCounter++;
    totalPages = ++curTotalPages;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    if (fd < 0) {
        if(DEBUG)  fprintf(stderr, "File not open!");
        return 0;
    }
    // initialize pageNum  
    if (totalPages < 0) {
        lseek(fd, 0, SEEK_SET);                                         // place position at the beginning
    unsigned long fileSize = lseek(fd, 0, SEEK_END);                // place position at the end
        totalPages  = fileSize / PAGE_SIZE;
        lseek(fd, 0, SEEK_SET);                                         // place position at the beginning
    }
    return (unsigned)totalPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}


RC FileHandle::readPageHeader(PageNum pageNum, void *data) {
    if (fd < 0) {
        if(DEBUG) fprintf(stderr, "File not open!");
        return -1;
    }
    if (pageNum >= (unsigned)totalPages) {
        if(DEBUG) fprintf(stderr, "Error in readPage: pageNum: %d >= total # of pages: %d.\n", pageNum, totalPages);
        return -1;
    }
    if (lseek(fd, pageNum * PAGE_SIZE, SEEK_SET) < 0) {
        if(DEBUG) perror("error in readHeader");
        return -1;
    }
    off_t bytesRead = read(fd, data, HEADER_SIZE);
    if (bytesRead == (off_t) -1) {
        if(DEBUG) perror("error in readHeader");
        return -1;
    }
    return 0;
}

