#include "pfm.h"
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
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
	int fd = open(fileName.c_str(), O_CREAT | O_WRONLY | O_EXCL, S_IRWXU | S_IRWXO);

	if (fd == -1) {
		fprintf(stderr, "File %s creation failed.\n", fileName.c_str());

		return -1;

	} else {

		fprintf(stdout, "File %s has been created.\n", fileName.c_str());

		return 0;
	}
    
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	int rc = remove(fileName.c_str());

	if (rc == 0) {

		fprintf(stdout, "File %s has been destroyed.\n", fileName.c_str());

	} else {

		fprintf(stderr, "File %s destroy failed.\n", fileName.c_str());
	}

    return rc;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (fileHandle.fd >= 0) {

		fprintf(stderr, "The fileHandle is already a handle for another open file!\n");

		return -1;
	}

	int fd = open(fileName.c_str(), O_RDWR);

	if (fd == -1) {

		fprintf(stderr, "File open failed!\n");

		return -1;

	}else {

		fileHandle.fd = fd;

		fprintf(stdout, "File %s has been opened successfully!\n", fileName.c_str());

		return 0;

	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{	
	int rc = close(fileHandle.fd);

	if (rc == 0) {

		fprintf(stdout, "File has been closed successfully!\n");

	}else {

		fprintf(stderr, "File close failed!\n");
	}

    return rc;
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

	if (fd == -1) {

		fprintf(stderr, "File has not been opened!\n");

		return -1;
	}

	unsigned totalNum = getNumberOfPages();

	if (pageNum >= totalNum) {

		fprintf(stderr, "Page does not exist!\n");

		return -1;

	}

	//locate offset to pageNum
	off_t seek = lseek(fd, pageNum * PAGE_SIZE, SEEK_SET);

	if (seek < 0) {

		fprintf(stderr, "Page reading failed!\n");

		return -1;
	}

	off_t bytesRead = read(fd, data, PAGE_SIZE);

	if (bytesRead == (off_t)-1) {

		fprintf(stderr, "Page reading failed!\n");

		return -1;
	}

	readPageCounter++;

    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{	
	if (fd == -1) {

		fprintf(stderr, "File has not been opened!\n");

		return -1;
	}

	unsigned totalNum = getNumberOfPages();

	if (pageNum >= totalNum) {

		fprintf(stderr, "Page does not exist!\n");

		return -1;

	}

	//locate offset to pageNum
	off_t seek = lseek(fd, pageNum * PAGE_SIZE, SEEK_SET);

	if (seek < 0) {

		fprintf(stderr, "Page writing failed!\n");

		return -1;
	}

	ssize_t bytesWrite = write(fd, data, PAGE_SIZE);

	if (bytesWrite < 0) {

		fprintf(stderr, "Page writing failed!\n");

		return -1;	

	}

	writePageCounter++;

    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if (fd == -1) {

		fprintf(stderr, "File has not been opened!\n");

		return -1;
	}

	off_t seek = lseek(fd, 0, SEEK_END);

	if (seek < 0) {

		fprintf(stderr, "Page appending failed!\n");

		return -1;
	}

	ssize_t bytesWrite = write(fd, data, PAGE_SIZE);

	if (bytesWrite < 0) {

		fprintf(stderr, "Page appending failed!bytesWrite\n");

		return -1;	

	}

	appendPageCounter++;

    return 0;
}


unsigned FileHandle::getNumberOfPages()
{	
	if (fd == -1) {

		fprintf(stderr, "File has not been opened!\n");

		return 0;
	}

	unsigned long fileSize = lseek(fd, 0, SEEK_END);

	unsigned totalNum = fileSize / PAGE_SIZE;

	lseek(fd, 0, SEEK_SET);

	fprintf(stdout, "Number of pages is %d.\n", totalNum);

    return totalNum;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;

    writePageCount = writePageCounter;

    appendPageCount = appendPageCounter;

    return 0;
}
