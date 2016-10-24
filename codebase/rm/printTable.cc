#include "rm_test_util.h"


int main(int argc, char** argv) {

	string tableName = argv[1];

	printf("Table[%s] is like this:\n", tableName.c_str());

	vector<Attribute> attrs;
    rm->getAttributes(tableName, attrs);

    for (int i = 0; i < attrs.size(); i++) {
    	printf("%s\t", attrs[i].name.c_str());
    }

    printf("\n");

	FileHandle fh;
    rbfm->openFile(tableName, fh);
    rbfm->printTable(fh, attrs);
    rbfm->closeFile(fh);

}