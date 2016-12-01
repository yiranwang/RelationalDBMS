
#include <cassert>
#include <cstring>
#include <cfloat>
#include "qe.h"
#include "qe_aux.cc"

Filter::Filter(Iterator* input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    rbfm = RecordBasedFileManager::instance();
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
    input->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data) {
    vector<Attribute> attrs;
    input->getAttributes(attrs);

    bool result = false;

    unsigned attrsSize = attrs.size();
    void *innerRecordFormat = malloc(PAGE_SIZE);
    short innerRecordSize = 0;

    short targetFieldNum = -1;

    for (int i = 0; i < attrsSize; i++) {

        if (strcmp(condition.lhsAttr.c_str(), attrs[i].name.c_str()) == 0) {
            targetFieldNum = i + 1;
            break;
        }
    }

    assert(targetFieldNum != -1);

    while (input->getNextTuple(data) != -1) {
        /*printf("\n-----------Filter getNextTuple Done!----------------------\n");

        printf("Filter data:  ");
        printAPIRecord(attrs, data);
        printf("\n");*/

        bool find = false;

        rbfm->composeInnerRecord(attrs, data, innerRecordFormat, innerRecordSize);

        short startOffset = *(short*)((char*)innerRecordFormat + sizeof(short) * targetFieldNum);

        short endOffset = 0;

        if (targetFieldNum == *(short*)((char*)innerRecordFormat)) {

            endOffset = innerRecordSize;

        }else {
            endOffset = *(short*)((char*)innerRecordFormat + sizeof(short) * (targetFieldNum + 1));
        }

        short length = endOffset - startOffset;

        // the field is NULL
        if (startOffset == -1) {
            find = opCompare(NULL, condition.rhsValue.data, condition.op, condition.rhsValue.type);

        }else {
            if(condition.rhsValue.type == TypeVarChar){
                void *field = malloc(length + sizeof(int));
                //*(int*)field = length;
                memcpy((char*)field, (char*)innerRecordFormat + startOffset, length + sizeof(int));
                find = opCompare(field, condition.rhsValue.data, condition.op, condition.rhsValue.type);
                free(field);
            }
            else{
                void *field = malloc(sizeof(int));
                memcpy((char*)field, (char*)innerRecordFormat + startOffset, 4);
                find = opCompare(field, condition.rhsValue.data, condition.op, condition.rhsValue.type);

                free(field);
            }
        }

        if (find) {
            result = true;
            break;
        }
    }

    if (!result) {
        return -1;
    }

    free(innerRecordFormat);
    //printf("\n-----------Filter getNextTuple before return 0!----------------------\n");
    return 0;
}

Project::Project(Iterator *input, const vector<string> &attrNames){
    this->input = input;
    this->projectAttrs = attrNames;
    rbfm = RecordBasedFileManager::instance();
}

RC Project::getNextTuple(void *data) {
    int rc = input->getNextTuple(data);
    //printf("\n-----------Project getNextTuple Done!----------------------\n");
    if(rc != 0) {
        return rc;
    }

    void* projectData = malloc(PAGE_SIZE);
    memset(projectData, 0, PAGE_SIZE);

    void* innerRecordFormat = malloc(PAGE_SIZE);
    short innerRecordSize = 0;

    //initial attributes from input
    vector<Attribute> attrs;
    input->getAttributes(attrs);

    rbfm->composeInnerRecord(attrs, data, innerRecordFormat, innerRecordSize);

    char* before = (char*)data;
    char* temp = (char*)projectData;

    unsigned offset = ceil((double)projectAttrs.size() / 8.0);

    for (int i = 0; i < projectAttrs.size(); i++) {

        int targetFieldNum = -1;

        // find the fields that need to be projected
        for (int j = 0; j < attrs.size(); j++){
            if (strcmp(projectAttrs[i].c_str(), attrs[j].name.c_str()) == 0){
                targetFieldNum = j;
                break;
            }
        }

        AttrType attrType  = attrs[targetFieldNum].type;
        int is_null = before[(int)(ceil((targetFieldNum + 1.0) / (8.0))) - 1] & (1 << (7 - (targetFieldNum % 8)));

        if(is_null != 0){
            temp[(int)(ceil((i + 1.0) / 8.0)) - 1] |= (1 << (7 - (i % 8)));

        }else{
            short startOffset = *(short*)((char*)innerRecordFormat + sizeof(short) * (targetFieldNum + 1));

            short endOffset = 0;

            if (targetFieldNum + 1 == *(short*)((char*)innerRecordFormat)) {

                endOffset = innerRecordSize;

            }else {
                endOffset = *(short*)((char*)innerRecordFormat + sizeof(short) * (targetFieldNum + 2));
            }

            short length = endOffset - startOffset;

            if(attrType == TypeVarChar){

                memcpy((char*)projectData + offset, (char*)innerRecordFormat + startOffset, length);
                offset += length;
            }
            else {
                assert(length == 4);
                memcpy((char*)projectData + offset, (char*)innerRecordFormat + startOffset, length);
                offset += length;
            }
        }

    }

    memcpy(data, projectData, offset);

    free(projectData);
    free(innerRecordFormat);
    //printf("\n-----------Project getNextTuple before return 0!----------------------\n");
    return 0;

}

bool findAttr(Attribute attribute, vector<string> attrs){
    for (int i = 0; i< attrs.size(); i++){
        if(strcmp(attribute.name.c_str(), attrs[i].c_str()) == 0){
            return true;
        }
    }
    return false;
}

void Project::getAttributes(vector<Attribute> &attrs) const{
    attrs.clear();
    vector<Attribute> attributes;
    input->getAttributes(attributes);

    for(int i = 0; i< attributes.size(); i++){

        if(findAttr(attributes[i], projectAttrs)) {
            attrs.push_back(attributes[i]);
        }
    }

}

void *unionLeft(void *left, void *right, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs);

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition,const unsigned numPages) {
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->condition = condition;
    this->memoryLimit = numPages * PAGE_SIZE;
    this->curMemoryUsage = 0;
    this->next = NULL;
    inmemoryMap.clear();
}

RC BNLJoin::getNextTuple(void *data) {
    string lhsAttr = condition.lhsAttr;
    string rhsAttr = condition.rhsAttr;

    void *tupleData = malloc(PAGE_SIZE);

    vector<Attribute> leftAttrs;
    leftIn->getAttributes(leftAttrs);

    vector<Attribute> rightAttrs;
    rightIn->getAttributes(rightAttrs);


    if (inmemoryMap.empty()) {

        //the record left from last memory loading
        if (next != NULL) {
            string key;
            getKeyAndValue(next, lhsAttr, leftAttrs, key);

            inmemoryMap.insert(pair<string, void*>(key, next));
            curMemoryUsage += calculateBytes(leftAttrs, next);

            //free(next);
            next = NULL;
        }

        // load block in leftIn to memory(store key value in map)
        while (curMemoryUsage < memoryLimit) {
            int rc = leftIn->getNextTuple(tupleData);

            if (rc == -1) {
                break;
            }

            unsigned tupleSize = calculateBytes(leftAttrs, tupleData);

            void *leftInTuple = malloc(tupleSize);
            memcpy(leftInTuple, tupleData, tupleSize);

            string key;
            getKeyAndValue(leftInTuple, lhsAttr, leftAttrs, key);

            //printf("%s", key.c_str());

            if (curMemoryUsage + tupleSize >= memoryLimit) {
                next = leftInTuple;
                break;
            }

            inmemoryMap.insert(pair<string, void*>(key, leftInTuple));
            curMemoryUsage += tupleSize;

            //free(leftInTuple);
        }
    }

    free(tupleData);

    if (inmemoryMap.empty()) {
        return -1;
    }


    // find matched pair in rightIn
    bool find = false;

    void *rightInTuple = malloc(PAGE_SIZE);

    vector<Attribute> attrs;
    this->getAttributes(attrs);

    while (rightIn->getNextTuple(rightInTuple) != -1) {
        string key;
        getKeyAndValue(rightInTuple, rhsAttr, rightAttrs, key);

        int keyCount = (int)inmemoryMap.count(key);
        multimap<string, void*>:: iterator iter = inmemoryMap.find(key);

        for (int i = 0; i < keyCount; i++) {
            void *leftInTuple = iter->second;
            iter++;


            void *unionData = unionLeft(leftInTuple, rightInTuple, leftAttrs, rightAttrs);
            unsigned unionDataSize = calculateBytes(attrs, unionData);

            memcpy(data, unionData, unionDataSize);
            free(unionData);
            find = true;
        }
        if (find) {
            break;
        }
    }
    free(rightInTuple);

    // if rightIn already runout, load another block of left
    if (!find) {
        for (multimap<string, void*>:: iterator iter = inmemoryMap.begin(); iter != inmemoryMap.end(); iter++) {

            free(iter->second);
        }

        inmemoryMap.clear();
        curMemoryUsage = 0;
        rightIn->setIterator();

        return this->getNextTuple(data);
    }

    return 0;
}


void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();

    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    for(int i =0; i < leftAttrs.size();i++){
        attrs.push_back(leftAttrs[i]);
    }
    for(int i = 0; i < rightAttrs.size(); i++){
        attrs.push_back(rightAttrs[i]);
    }
}


void *unionLeft(void *left, void *right, vector<Attribute> leftAttrs, vector<Attribute> rightAttrs) {
    unsigned leftTupleSize = calculateBytes(leftAttrs, left);
    unsigned rightTupleSize = calculateBytes(rightAttrs, right);

    void *result = malloc(leftTupleSize + rightTupleSize);
    memset(result, 0, leftTupleSize + rightTupleSize);

    char* temp = (char*)left; // read left first
    unsigned offset = ceil((double)(leftAttrs.size() + rightAttrs.size()) / 8.0);
    unsigned leftOffset = ceil((double)leftAttrs.size() / 8.0);
    unsigned rightOffset = ceil((double)rightAttrs.size() / 8.0);

    int i = 0;

    for (i = 0; i < leftAttrs.size(); i++) {

        int isNull = temp[(int)(ceil((i + 1.0) / 8.0)) - 1] & (1 << (7 - (i % 8)));

        if(isNull != 0) {
            *((char*)result + (int)(ceil((i + 1.0) / 8.0)) - 1) |= (1 << (7 - (i % 8)));
            continue;
        }

        if(leftAttrs[i].type == TypeVarChar){
            int len = *(int*)((char*)left + leftOffset);

            *(int*)((char*)result + offset) = len;
            offset += sizeof(int);
            leftOffset += sizeof(int);
            memcpy((char*)result + offset, (char*)left + leftOffset, len);

            offset += len;
            leftOffset += len;

        } else {
            int len = sizeof(int);
            memcpy((char*)result + offset, (char*)left + leftOffset, len);

            offset += len;
            leftOffset += len;
        }
    }

    // concat right data
    temp = (char*)right;
    for (; i < leftAttrs.size() + rightAttrs.size(); i++) {

        int j = i - leftAttrs.size();

        int isNull = temp[(int)(ceil((j + 1.0) / 8.0)) - 1] & (1 << (7 - (j % 8)));

        if(isNull != 0) {
            *((char*)result + (int)(ceil((i + 1.0) / 8.0)) - 1) |= (1 << (7 - (i % 8)));
            continue;
        }

        if(rightAttrs[j].type == TypeVarChar){
            int len = *(int*)((char*)right + rightOffset);

            *(int*)((char*)result + offset) = len;
            offset += sizeof(int);
            rightOffset += sizeof(int);

            memcpy((char*)result + offset, (char*)right + rightOffset, len);

            offset += len;
            rightOffset += len;

        } else {
            int len = sizeof(int);
            memcpy((char*)result + offset, (char*)right + rightOffset, len);

            offset += len;
            rightOffset += len;
        }
    }
    return result;
}


INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    this->leftIn = leftIn;
    this->rightIn= rightIn;
    this->condition = condition;
}

void* generateKey(const void *data, string attriname, vector<Attribute> attrs) {

    int targetFieldNum = -1;

    // find the attribute field number count
    for (int i = 0; i < attrs.size(); i++) {
        if (strcmp(attriname.c_str(), attrs[i].name.c_str()) == 0) {
            targetFieldNum = i;
            break;
        }
    }

    char *temp = (char*)data;

    int isNull = temp[(int)(ceil((targetFieldNum + 1.0) / 8.0)) - 1] & (1 << (7 - (targetFieldNum % 8)));

    if (isNull != 0) {
        return NULL;
    }

    short offset = ceil((double)attrs.size() / 8.0); // nullIndicator

    for (int i = 0; i < targetFieldNum; i++) {
        isNull = temp[(int)(ceil((i + 1.0) / 8.0)) - 1] & (1 << (7 - (i % 8)));

        if (isNull != 0) {
            continue;
        }else {

            if (attrs[i].type == TypeVarChar) {
                int len = *(int*)((char*)data + offset);
                offset += len + sizeof(int);

            }else {
                offset += sizeof(int);
            }
        }
    }

    void *result;

    if (attrs[targetFieldNum].type == TypeVarChar) {
        int len = *(int*)((char*)data + offset);

        result = malloc(len + sizeof(int));
        memcpy((char*)result, (char*)data + offset, len + sizeof(int));

    }else {
        result = malloc(sizeof(int));
        memcpy((char*)result, (char*)data + offset, sizeof(int));

    }

    return result;
}

RC INLJoin::getNextTuple(void *data) {

    string lhsAttr = condition.lhsAttr;
    string rhsAttr = condition.rhsAttr;

    vector<Attribute> leftAttrs;
    leftIn->getAttributes(leftAttrs);

    vector<Attribute> rightAttrs;
    rightIn->getAttributes(rightAttrs);

    vector<Attribute> attrs;
    this->getAttributes(attrs);

    void *leftTupleData = malloc(PAGE_SIZE);
    void *rightTupleData = malloc(PAGE_SIZE);

    bool find = false;

    queue<void*> queue;

    if (!queue.empty()) {
        void *unionData = queue.front();

        int unionDataSize = calculateBytes(attrs, unionData);
        memcpy(data, unionData, unionDataSize);

        queue.pop();

        free(leftTupleData);
        free(rightTupleData);

        free(unionData);

        return 0;
    }

    while (1) {

        int rc = leftIn->getNextTuple(leftTupleData);

        /*printf("\n-----------INJoin getNextTuple Done!----------------------\n");

        printf("leftTupleData:");
        printAPIRecord(leftAttrs, leftTupleData);*/

        if (rc == -1) {
            break;
        }


        void *leftKey = generateKey(leftTupleData, lhsAttr, leftAttrs);

        rightIn->setIterator(leftKey, leftKey, true, true);

        while (rightIn->getNextTuple(rightTupleData) != EOF) {
            void *unionData = unionLeft(leftTupleData, rightTupleData, leftAttrs, rightAttrs);

            if (!find) {
                int unionDataSize = calculateBytes(attrs, unionData);
                memcpy(data, unionData, unionDataSize);

                //free(unionData);
                find = true;
            }else {
                queue.push(unionData);
            }

        }

        if (leftKey != NULL) {
            free(leftKey);
        }

        if (find) {
            break;
        }
    }

    /*printf("\nrightTupleData:");
    printAPIRecord(rightAttrs, rightTupleData);*/

    if (!find) {
        /*printf("--------------------------------------------------------------------");
        printf("leftTupleData:");
        printAPIRecord(leftAttrs, leftTupleData);
        printf("\nrightTupleData:");
        printAPIRecord(rightAttrs, rightTupleData);*/

        free(leftTupleData);
        free(rightTupleData);

        return EOF;
    }

    free(leftTupleData);
    free(rightTupleData);

    return 0;

}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
    attrs.clear();

    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    for(int i = 0; i < leftAttrs.size();i++){
        attrs.push_back(leftAttrs[i]);
    }
    for(int i = 0; i < rightAttrs.size(); i++){
        attrs.push_back(rightAttrs[i]);
    }
}


GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions){
    this->build = false;
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->condition = condition;
    this->numPartitions = numPartitions;

    vector<Attribute>leftAttrs;
    this->leftIn->getAttributes(leftAttrs);
    string tmp = leftAttrs[0].name;
    this->left_suffix = tmp.substr(0,leftAttrs[0].name.find("."));

    vector<Attribute>rightAttrs;
    this->rightIn->getAttributes(rightAttrs);
    tmp = rightAttrs[0].name;
    this->right_suffix = tmp.substr(0,tmp.find("."));

    this->inMemoryName = "";
    this->streamName = "";
    while (!this->resultQueue.empty()){
        resultQueue.pop();
    }
    this->inmemoryMap.clear();
    this->nextPartitionNum = 0;
}

void GHJoin::buildPartition() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    string leftAttr = condition.lhsAttr;
    string rightAttr = condition.rhsAttr;

    for (int i = 0; i < numPartitions; i++) {
        string leftName = "left_" + left_suffix + "_" + intToString(i);
        rbfm->createFile(leftName);

        string rightName = "right_" + right_suffix + "_" + intToString(i);
        rbfm->createFile(rightName);
    }

    void *tupleData = malloc(PAGE_SIZE);

    hash<string> getHashVal; // used to get hash value of key
    RID rid;

    // build partition of leftIn, create rbfm files
    while (leftIn->getNextTuple(tupleData) != EOF) {

        string key;
        getKeyAndValue(tupleData, leftAttr, leftAttrs, key);

        int index = getHashVal(key) % numPartitions;
        string leftName = "left_" + left_suffix + "_" + intToString(index);

        FileHandle fileHandle;
        rbfm->openFile(leftName, fileHandle);
        rbfm->insertRecord(fileHandle, leftAttrs, tupleData, rid);
        rbfm->closeFile(fileHandle);

    }

    // build partition of rightIn, create rbfm files
    while (rightIn->getNextTuple(tupleData) != EOF) {

        string key;
        getKeyAndValue(tupleData, rightAttr, rightAttrs, key);

        int index = getHashVal(key) % numPartitions;
        string rightName = "right_" + right_suffix + "_" + intToString(index);

        FileHandle fileHandle;
        rbfm->openFile(rightName, fileHandle);
        rbfm->insertRecord(fileHandle, rightAttrs, tupleData, rid);
        rbfm->closeFile(fileHandle);

    }

    free(tupleData);
    build = true;
    return;
}


RC GHJoin::getNextTuple(void *data) {
    if (!build) {
        buildPartition();
    }

    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;
    vector<Attribute> attrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);
    this->getAttributes(attrs);

    string leftAttr = condition.lhsAttr;
    string rightAttr = condition.rhsAttr;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    if (!resultQueue.empty()) {
        void *headData = resultQueue.front();

        int headDataLen = calculateBytes(attrs, headData);
        memcpy(data, headData, headDataLen);

        resultQueue.pop();
        free(headData);

        return 0;
    }


    if (inmemoryMap.empty()) {
        if (nextPartitionNum >= numPartitions) {
            return EOF;
        }
        loadData(); //build inmemory hash table using inMemoryName table
    }


    bool find = false;
    RID rid;

    void* tupleData = malloc(PAGE_SIZE);

    while (GHJ_rbfm_scanIterator->getNextRecord(rid, tupleData) != EOF) {
        string streamNameTemp = streamName.substr(0, streamName.find("_"));

        string inMemoryAttr;
        string streamAttr;

        // stream data coming from leftIn
        if (strcmp(streamNameTemp.c_str(), "left") == 0) {
            inMemoryAttr = rightAttr;
            streamAttr = leftAttr;

        }else {
            inMemoryAttr = leftAttr;
            streamAttr = rightAttr;
        }

        /*printf("\nstreamName: %s\n", streamNameTemp.c_str());
        printAPIRecord(streamAttrs, tupleData);
        printf("\n");*/

        string key;
        getKeyAndValue(tupleData, streamAttr, streamAttrs, key);

        //printf("\nkey is: %s\n", key.c_str());

        int keyCount = (int)inmemoryMap.count(key);
        auto iter = inmemoryMap.find(key);

        for (int i = 0; i < keyCount; i++) {
            void* unionData;
            if (strcmp(streamNameTemp.c_str(), "left") == 0) {

                unionData = unionLeft(tupleData, iter->second, streamAttrs, inMemoryAttrs);

            }else {
                unionData = unionLeft(iter->second, tupleData, inMemoryAttrs, streamAttrs);
            }

            int unionDataLen = calculateBytes(attrs, unionData);

            void* queueData = malloc(unionDataLen);
            memcpy(queueData, unionData, unionDataLen);

            resultQueue.push(queueData);

            free(unionData);
            find = true;
        }

        if (find) {
            break;
        }
    }
    free(tupleData);

    if (find) {
        return this->getNextTuple(data);

    }else {
        for (auto iter = inmemoryMap.begin(); iter!= inmemoryMap.end(); iter++){
            free(iter->second);
        }
        inmemoryMap.clear();

        GHJ_rbfm_scanIterator->close();
        delete(GHJ_rbfm_scanIterator);

        rbfm->closeFile(GHJ_fileHandle);


        string leftname = "left_" + this->left_suffix + "_" + intToString(nextPartitionNum - 1);
        string rightname = "right_" + this->right_suffix + "_" + intToString(nextPartitionNum - 1);

        rbfm->destroyFile(leftname);
        rbfm->destroyFile(rightname);

        return this->getNextTuple(data);
    }

    return 0;
}


void GHJoin::loadData() {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RecordBasedFileManager *rbfmScan = RecordBasedFileManager::instance();

    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    string leftAttr = condition.lhsAttr;
    string rightAttr = condition.rhsAttr;

    string leftName = "left_" + left_suffix + "_" + intToString(nextPartitionNum);
    string rightName = "right_" + right_suffix + "_" + intToString(nextPartitionNum);

    FileHandle fileHandle;
    rbfm->openFile(leftName, fileHandle);
    int leftPartitionPages = fileHandle.getNumberOfPages(); // number of pages in each partition file
    rbfm->closeFile(fileHandle);

    rbfm->openFile(rightName, fileHandle);
    int rightPartitionPages = fileHandle.getNumberOfPages(); // number of pages in each partition file
    rbfm->closeFile(fileHandle);

    // used left partition as hash table in memory
    if (leftPartitionPages <= rightPartitionPages) {
        inMemoryName = leftName;
        streamName = rightName;
        streamAttrs = rightAttrs;
        inMemoryAttrs = leftAttrs;

        buildHashTable(rbfm, rbfmScan, leftAttr);

    // used right partition as hash table in memory
    }else {
        inMemoryName = rightName;
        streamName = leftName;
        streamAttrs = leftAttrs;
        inMemoryAttrs = rightAttrs;

        buildHashTable(rbfm, rbfmScan, rightAttr);

    }

    GHJ_rbfm_scanIterator = new RBFM_ScanIterator();

    vector<string> streamAttrNames;
    for (int i = 0; i < streamAttrs.size(); i++) {
        streamAttrNames.push_back(streamAttrs[i].name);
    }

    //FileHandle fileHandle;
    rbfm->openFile(streamName, GHJ_fileHandle);

    rbfmScan->scan(GHJ_fileHandle, streamAttrs, "", NO_OP, NULL, streamAttrNames, *GHJ_rbfm_scanIterator);
}


void GHJoin::buildHashTable(RecordBasedFileManager *rbfm, RecordBasedFileManager *rbfmScan, string inMemoryAttr) {

    vector<string> inMemoryAttrNames;

    for (int i = 0; i < inMemoryAttrs.size(); i++) {
        inMemoryAttrNames.push_back(inMemoryAttrs[i].name);
    }

    FileHandle fileHandle;
    rbfm->openFile(inMemoryName, fileHandle);

    RBFM_ScanIterator inMemoryRbfmScanIterator;
    rbfmScan->scan(fileHandle, inMemoryAttrs, "", NO_OP, NULL, inMemoryAttrNames, inMemoryRbfmScanIterator);

    void *tupleData = malloc(PAGE_SIZE);
    RID rid;

    // build inmemory hash table
    while (inMemoryRbfmScanIterator.getNextRecord(rid, tupleData) != EOF) {

        /*printf("\ninMemoryName: %s\n", inMemoryName.c_str());
        printAPIRecord(inMemoryAttrs, tupleData);
        printf("\n");*/

        string key;
        getKeyAndValue(tupleData, inMemoryAttr, inMemoryAttrs, key);

        int tupleDataLen = calculateBytes(inMemoryAttrs, tupleData);
        void *inMemoryData = malloc(tupleDataLen);
        memcpy(inMemoryData, tupleData, tupleDataLen);

        inmemoryMap.insert(pair<string, void*>(key, inMemoryData));
    }

    inMemoryRbfmScanIterator.close();
    rbfm->closeFile(fileHandle);
    free(tupleData);

    nextPartitionNum++;
}


void GHJoin::getAttributes(vector<Attribute> &attrs) const{
    attrs.clear();
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    for(int i =0; i < leftAttrs.size();i++){
        attrs.push_back(leftAttrs[i]);
    }
    for(int i = 0; i < rightAttrs.size(); i++){
        attrs.push_back(rightAttrs[i]);
    }
}


// self defined comparator for aggregation
struct AggreMapComparator {
    bool operator() (const KeyAndValue& left, const KeyAndValue& right) const
    {
        if(left.type == TypeVarChar){
            return left.key.compare(right.key) < 0;
        }
        else{
            return left.keyFloat < right.keyFloat;
        }
    }
};

// maintain the aggregate value
struct AggreOpValue {
    float max;
    float min;
    int count;
    double sum;
};


Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op){
    this->input = input;
    this->groupby = false;
    this->aggAttr = aggAttr;
    this->op = op;
    this->resultSize = 1;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, Attribute groupAttr, AggregateOp op){
    this->input = input;
    this->groupAttr = groupAttr;
    this->aggAttr = aggAttr;
    this->op = op;
    this->groupby = true;
    this->resultSize = -1;
    while(!this->resultQueue.empty()){
        this->resultQueue.pop();
    }
}

void updateAggreOpValue(AggreOpValue &before, KeyAndValue value) {
    if (strcmp(value.key.c_str(), "1") == 0) {
        return;
    }
    float keyFloat = value.keyFloat;
    if (keyFloat > before.max) {
        before.max = keyFloat;
    }
    if (keyFloat < before.min) {
        before.min = keyFloat;
    }
    before.count++;
    before.sum += keyFloat;
}

float getAggreOpValue(AggregateOp op, AggreOpValue value) {
    if (op == MAX) {
        return value.max;

    } else if (op == MIN) {
        return value.min;

    } else if (op == COUNT) {
        return value.count;

    } else if (op == SUM) {
        return value.sum;

    }else {
        float avg = (value.sum / (float)value.count);
        return avg;
    }
}

// caculate aggregate value by groupby
void Aggregate::groupByAggregate() {
    vector<Attribute> attrs;
    input->getAttributes(attrs);

    string groupAttrName = groupAttr.name;
    string aggAttrName = aggAttr.name;

    // order the key according to the self defined comparator
    map<KeyAndValue, AggreOpValue, AggreMapComparator> inmemoryMap;

    void *tupleData = malloc(PAGE_SIZE);

    while (input->getNextTuple(tupleData) != EOF) {
        string key; // just as an argument
        KeyAndValue groupAttrKey = getKeyAndValue(tupleData, groupAttrName, attrs, key);
        KeyAndValue aggAttrValue = getKeyAndValue(tupleData, aggAttrName, attrs, key);

        // case NULL:
        if (strcmp(groupAttrKey.key.c_str(), "1") == 0) {
            continue;
        }

        auto iter = inmemoryMap.find(groupAttrKey);

        // if the map already contains the groupby attribute, update the aggregate value
        if (iter != inmemoryMap.end()) {
            updateAggreOpValue(iter->second, aggAttrValue);

        }else {
            AggreOpValue newValue;
            newValue.max = aggAttrValue.keyFloat;
            newValue.min = aggAttrValue.keyFloat;
            newValue.count = 1;
            newValue.sum = aggAttrValue.keyFloat;

            inmemoryMap.insert(pair<KeyAndValue, AggreOpValue>(groupAttrKey, newValue));
        }
    }
    free(tupleData);

    // compose the returned data and push into result queue
    for (auto iter = inmemoryMap.begin(); iter != inmemoryMap.end(); iter++) {
        KeyAndValue groupAttrKey = iter->first;
        AggreOpValue aggValue = iter->second;

        void *returnData;

        if (groupAttrKey.type == TypeVarChar) {
            string key = groupAttrKey.key;

            int keyLen = (int)key.length();
            int returnDataLen = 1 + sizeof(int) + keyLen + sizeof(int);

            returnData = malloc(returnDataLen);
            memset(returnData, 0, returnDataLen);

            *(int*)((char*)returnData + 1) = keyLen;
            memcpy((char*)returnData + 1 + sizeof(int), key.c_str(), keyLen);

            *(float*)((char*)returnData + 1 + sizeof(int) + keyLen) = getAggreOpValue(op, aggValue);

            resultQueue.push(returnData);

        }else if (groupAttrKey.type == TypeInt){
            int keyValue = (int)groupAttrKey.keyFloat;

            int returnDataLen = 1 + sizeof(int) + sizeof(float);

            returnData = malloc(returnDataLen);
            memset(returnData, 0, returnDataLen);

            *(int*)((char*)returnData + 1) = keyValue;
            *(float*)((char*)returnData + 1 + sizeof(int)) = getAggreOpValue(op, aggValue);

            resultQueue.push(returnData);

        }else {
            float keyValue = groupAttrKey.keyFloat;

            int returnDataLen = 1 + sizeof(float) + sizeof(float);

            returnData = malloc(returnDataLen);
            memset(returnData, 0, returnDataLen);

            *(float*)((char*)returnData + 1) = keyValue;
            *(float*)((char*)returnData + 1 + sizeof(float)) = getAggreOpValue(op, aggValue);

            resultQueue.push(returnData);
        }
    }
    resultSize = (int)resultQueue.size();
}

RC Aggregate::getNextTuple(void *data) {

    if (resultSize == 0) { //no next tuple returned
        return EOF;
    }

    // do groupby aggregate
    if (groupby) {
        if (resultSize == -1) { //not initialize groupby
            groupByAggregate();
            return this->getNextTuple(data);

        }
        if (!resultQueue.empty()) {
            void* headData = resultQueue.front();

            vector<Attribute> attrs;
            this->getAttributes(attrs);

            int headDataSize = calculateBytes(attrs, headData);

            memcpy(data, headData, headDataSize);
            resultQueue.pop();
            resultSize--;

            return 0;
        }else {
            return EOF;
        }

        // no groupby, just aggregate
    }else {
        string aggAttrName = aggAttr.name;

        void *tupleData = malloc(PAGE_SIZE);

        vector<Attribute> attrs;
        input->getAttributes(attrs);

        AggreOpValue aggreOpValue;
        aggreOpValue.max = -FLT_MAX;
        aggreOpValue.min = FLT_MAX;
        aggreOpValue.count = 0;
        aggreOpValue.sum = 0.0;

        while (input->getNextTuple(tupleData) != EOF) {
            void *key = generateKey(tupleData, aggAttrName, attrs);

            if (key == NULL) {
                continue;
            }
            if (aggreOpValue.count == 99) {

            }
            if (aggAttr.type == TypeInt) {
                int val = *(int*)key;
                if (val > aggreOpValue.max) {
                    aggreOpValue.max = val;
                }
                if (val < aggreOpValue.min) {
                    aggreOpValue.min = val;
                }
                aggreOpValue.sum += val;

            }else if (aggAttr.type == TypeReal) {
                float val = *(float*)key;
                if (val > aggreOpValue.max) {
                    aggreOpValue.max = val;
                }
                if (val < aggreOpValue.min) {
                    aggreOpValue.min = val;
                }
                aggreOpValue.sum += val;
            }
            aggreOpValue.count++;

            free(key);
        }
        free(tupleData);

        if (aggreOpValue.count > 0) {
            memset(data, 0, 1);
            resultSize--;

            float opResult = getAggreOpValue(op, aggreOpValue);

            memcpy((char*)data + 1, &opResult, 4);
            return 0;

        }else {
            resultSize--;
            *(char*)data |= (1 << 7);

            return 0;
        }
    }
    return 0;
}

string attriNameTrans(string before, AggregateOp op){
    string res = "";
    if(op == MAX){
        res = "MAX(";
    }
    else if(op == MIN){
        res = "MIN(";
    }
    else if(op == AVG){
        res = "AVG(";
    }
    else if(op == COUNT){
        res = "COUNT(";
    }
    else {
        res = "SUM(";
    }
    res = res + before + ")";
    return res;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
    if(!groupby){
        Attribute attribute;
        attribute.name = this->aggAttr.name;
        attribute.name = attriNameTrans(attribute.name, this->op);
        attribute.length = 4;
        attribute.type = TypeReal;
        attrs.clear();
        attrs.push_back(attribute);
    }
    else {
        attrs.clear();
        Attribute groupAttri;
        groupAttri = this->groupAttr;
        attrs.push_back(groupAttri); // first group

        Attribute attribute;
        attribute.name = this->aggAttr.name;
        attribute.name = attriNameTrans(attribute.name, this->op);
        attribute.length = 4;
        attribute.type = TypeReal;// then the aggregate
        attrs.push_back(attribute);
    }
}