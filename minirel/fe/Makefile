# Makefile for FE layer
LIB	= fe
INCDIR	= ${MINIREL_HOME}/h
INCS	= ../am/libam.a ../hf/libhf.a ../pf/libpf.a ../bf/libbf.a
SRCS	= fe.c
TESTS	= fetest-ddl.c fetest-dml.c
OBJS	= ${SRCS:.c=.o}
LIBS	= lib${LIB}.a ../am/libam.a ../hf/libhf.a ../pf/libpf.a ../bf/libbf.a

#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -g -ansi -pedantic
#CFLAGS	= -O -ansi -pedantic
#############################################################################

all: lib${LIB}.a ${LIB}test-ddl ${LIB}test-dml

${LIB}test-ddl: ${LIB}test-ddl.o lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ $< ${LIBS}

${LIB}test-dml: ${LIB}test-dml.o lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ $< ${LIBS}

lib${LIB}.a: $(OBJS)
	ar cr lib${LIB}.a $(OBJS)
	ranlib lib${LIB}.a

../am/libam.a: ../am/am.c
	cd ../am && $(MAKE)

../hf/libhf.a: ../hf/hf.c
	cd ../hf && $(MAKE)

../pf/libpf.a: ../pf/pf.c
	cd ../pf && $(MAKE)

../bf/libbf.a: ../bf/bf.c
	cd ../bf && $(MAKE)

$(OBJS): ${INCS}

clean:
	rm -f lib${LIB}.a *.o ${LIB}test-ddl ${LIB}test-dml *.bak *~

cleanall:
	make clean
	cd ../bf && make clean
	cd ../pf && make clean
	cd ../hf && make clean
	cd ../am && make clean

.c.o:; $(CC) $(CFLAGS) -c $< -I. -I$(INCDIR)

run:
	rm -fr testdb; make clean; make; ./${LIB}test-ddl; ./${LIB}test-ddl > ${LIB}test-ddl.out; #diff ${LIB}test.out ${LIB}test.out.orig
