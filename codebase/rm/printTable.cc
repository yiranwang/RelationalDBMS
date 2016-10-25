#include "rm_test_util.h"
#include <unistd.h>


int main(int argc, char** argv) {

	string tableName = argv[1];


  	if (access(argv[1], F_OK ) == -1) {
  		printf("talbe %s does not exist!\n", argv[1]);
  		return -1;
  	}

	printTable(tableName);

	return 0;

}