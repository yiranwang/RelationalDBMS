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
    //If O_CREAT and O_EXCL are set, open() shall fail if the file exists
    int fd = open(fileName.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        perror("createFile");
        return(-1);
    }        
    fprintf(stdout, "Successfully created file: %s.\n", fileName.c_str());
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{ 
    if (fileHandle.fd > 0) {
        fprintf(stderr, "Error! FileHandle is already a handle of an open file\n!");
        return -1;
    }

    int fd = open(fileName.c_str(), O_RDWR, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        perror("openFile");
        return -1;
    } else {
        fileHandle.fd = fd;
        fprintf(stdout, "Successfully opened file. fd is: %d\n", fd);
        fflush(stdout);
        return 0;
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return close(fileHandle.fd);
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fd = -1;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if (fd < 0) {
        fprintf(stderr, "File not open!");
        return -1;
    }
    unsigned totalPages = getNumberOfPages();
    if (pageNum >= totalPages) {
        fprintf(stderr, "Error in readPage: pageNum: %d > total # of pages: %d.\n", pageNum, totalPages);
        return -1;
    }
    if (lseek(fd, 0, SEEK_SET) < 0) {
        fprintf(stderr, "Error in readPage: failed to locate the page!\n");
        return -1;
    }
    off_t bytesRead = read(fd, data, PAGE_SIZE);
    perror("Reading file error");
    if (bytesRead == (off_t) -1) {
        fprintf(stderr, "Error in readPage: failed to read file!\n");
        return -1;
    }
    if((int)bytesRead != PAGE_SIZE) {
        fprintf(stderr, "Error in readPage: Bytes Read: %d, does not match PageSize: %d\n", (int)bytesRead, PAGE_SIZE);
        return -1;
    }
    readPageCounter++;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (fd < 0) {
        fprintf(stderr, "File not open!");
        return -1;
    }
    unsigned totalPages = getNumberOfPages();
    if (pageNum >= totalPages) {
        fprintf(stderr, "Error in writePage: pageNum: %d > total # of pages: %d.\n", pageNum, totalPages);
        return -1;
    }
    lseek(fd, pageNum * PAGE_SIZE, SEEK_SET);
    if (write(fd, data, PAGE_SIZE) < 0) {
        fprintf(stderr, "Error in writePage: write failed!\n");
    }
    writePageCounter++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if (fd < 0) {
        fprintf(stderr, "File not open!");
        return -1;
    }
    lseek(fd, 0, SEEK_END);                                            // place position at the end
    if (write(fd, data, PAGE_SIZE) < 0) {
        fprintf(stderr, "Error in appendPage: write failed!\n");
        return -1;
    }
    appendPageCounter++;
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    if (fd < 0) {
        fprintf(stderr, "File not open!");
        return 0;
    }
    unsigned long fileSize = lseek(fd, 0, SEEK_END);                // place position at the end
    unsigned pageNumber  = fileSize / PAGE_SIZE;
    lseek(fd, 0, SEEK_SET);                                         // place position at the beginning
    fprintf(stderr, "number of page is %d.\n", pageNumber);
    return pageNumber;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}

