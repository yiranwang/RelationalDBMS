#include "pfm.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
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
    fd = 0;
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    //If O_CREAT and O_EXCL are set, open() shall fail if the file exists
    fd = open(fileName.c_str(), O_WRONLY | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd == -1) {
        fprintf(stderr, "Error! Path: %s already exists!\n", fileName.c_str());
        return(-1);
    } else {
        FileHandle fileHandle;
        fileHandle.fd = fd;
        
        return 0;
    }
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{ 
    fd = open(fileName.c_str(), O_WRONLY);
    if (fd == -1) {
        fprintf(stderr, "Error! Path: %s cannot open!\n", fileName.c_str());
        return(-1);
    } else {
        fileHandle.fd = fd;
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
    fd = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
