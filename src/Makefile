##############
# Make the project
# By qinjianbin
##############


PROD	:= PROD
OPT     := -O3
VERSION := \"0.0.1.0_${PROD}\"
TARGETS := chunk_ed_join cbdselect charstat
DEFINES := -DCASE_INSENSITIVE -DCUTS_BY_RATE -DFULL_VCN
SRCS    := hash_dict.c bound.c filter.c index.c joins.c records.c chunk_ed_join.c chunk_bound_dict.c usage.c cbdselect.c charstat.c
OBJS    := ${SRCS:.c=.o}

CCFLAGS = ${OPT} -Wall -Wno-deprecated -ggdb -D${PROD} ${DEFINES} -DVERSION=${VERSION} 
LDFLAGS = ${OPT} -ggdb
LIBS    = -lcrypto
CC	= g++


.PHONY: all clean distclean 
all:: ${TARGETS} 


chunk_ed_join: hash_dict.o bound.o index.o joins.o records.o chunk_ed_join.o filter.o chunk_bound_dict.o usage.o
	${CC} ${LDFLAGS} -o $@ $^ ${LIBS} 

cbdselect: cbdselect.o chunk_bound_dict.o hash_dict.o
	${CC} ${LDFLAGS} -o $@ $^ ${LIBS}

charstat: charstat.o
	${CC} ${LDFLAGS} -o $@ $^

${OBJS}: %.o: %.c
	${CC} ${CCFLAGS} -o $@ -c $< 

clean:: 
	-rm -f *~ *.o ${TARGETS}

distclean:: clean
