include ../makefile.inc

all: librbf.a 

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h
rbfm_aux.o: rbfm.h
rbfm_scan.o: rbfm.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)
librbf.a: librbf.a(rbfm_aux.o)
librbf.a: librbf.a(rbfm_scan.o)


# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm *.a *.o *~
