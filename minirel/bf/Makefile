# Makefile for BF layer
LIB	= bf
INCDIR	= ${MINIREL_HOME}/h
INCS	=
SRCS	= bf.c
TESTS	= bftest.c
OBJS	= ${SRCS:.c=.o}
LIBS	= lib${LIB}.a #../pf/libpf.a

#############################################################################
# This macro definition can be overwritten by command-line definitions.
CC	= gcc
CFLAGS	= -g -ansi -pedantic
#CFLAGS	= -O -ansi -pedantic
#############################################################################

#all: lib${LIB}.a ${LIB}test shsontest
all: lib${LIB}.a ${LIB}test

${LIB}test: ${LIB}test.o ${LIBS}
	$(CC) $(CFLAGS) -o $@ $< ${LIBS}

../pf/libpf.a:
	cd ../pf && $(MAKE)

#shsontest: shsontest.o ${LIBS}
#	$(CC) $(CFLAGS) -o $@ $< ${LIBS}

lib${LIB}.a:$(OBJS)
	ar cr lib${LIB}.a $(OBJS)
	ranlib lib${LIB}.a

$(OBJS): ${INCS}

clean:
	rm -f lib${LIB}.a *.o ${LIB}test *.bak *~

.c.o:
	$(CC) $(CFLAGS) -c $< -I. -I$(INCDIR)

run:
	make clean; make; ./bftest; ./bftest > bftest.out; diff bftest.out bftest.out.orig
