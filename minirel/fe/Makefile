# Makefile for FE layer
LIB	= fe
INCDIR	= ${MINIREL_HOME}/h
INCS	=
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

../am/libam.a:
	cd ../am && $(MAKE)

../hf/libhf.a:
	cd ../hf && $(MAKE)

$(OBJS): ${INCS}

clean:
	rm -f lib${LIB}.a *.o ${LIB}test *.bak *~

.c.o:; $(CC) $(CFLAGS) -c $< -I. -I$(INCDIR)
