# Makefile for HF layer
LIB	= hf
INCDIR	= ${MINIREL_HOME}/h
INCS	= ../pf/libpf.a ../bf/libbf.a
SRCS	= hf.c
TESTS	= hftest.c
OBJS	= ${SRCS:.c=.o}
LIBS	= lib${LIB}.a ../pf/libpf.a ../bf/libbf.a

#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -g -ansi -pedantic
#CFLAGS	= -O -ansi -pedantic
#############################################################################

all: lib${LIB}.a ${LIB}test

${LIB}test: ${LIB}test.o lib${LIB}.a
	$(CC) $(CFLAGS) -o $@ $< ${LIBS}

../pf/libpf.a: ../pf/pf.c
	cd ../pf && $(MAKE)

../bf/libbf.a: ../bf/bf.c
	cd ../bf && $(MAKE)

lib${LIB}.a: $(OBJS)
	ar cr lib${LIB}.a $(OBJS)
	ranlib lib${LIB}.a

$(OBJS): ${INCS}

clean:
	cd ../pf && make clean
	rm -f lib${LIB}.a *.o ${LIB}test *.bak *~

.c.o:; $(CC) $(CFLAGS) -c $< -I. -I$(INCDIR)

run:
	make clean; make; ./hftest; ./hftest > hftest.out; #diff hftest.out hftest.out.orig
