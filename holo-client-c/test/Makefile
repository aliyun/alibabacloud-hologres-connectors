BIN=test
CC=gcc
CFLAGS=-lpthread
STATCI_LIBS=-L../src/lib -llog4c -L/usr/local/Cellar/cunit/2.1-3/lib -lcunit #Link liblog4c.a 
SHARED_LIBS=-L/usr/local/opt/postgresql@11/lib -lpq #Link libpq.so
INCS=-I/usr/local/opt/postgresql@11/include -I../src/include -I../include -I../src -I/usr/local/Cellar/cunit/2.1-3/include #Header file directory
SRCS:= \
	unit_test.c \
	../src/action.c \
	../src/batch.c \
	../src/connection_holder.c \
	../src/direct_collector.c \
	../src/future.c \
	../src/get_collector.c \
	../src/holo_client.c \
	../src/holo_config.c \
	../src/logger.c \
	../src/lp_map.c \
	../src/meta_cache.c \
	../src/murmur3.c \
	../src/mutation_collector.c \
	../src/mutation_map.c \
	../src/record.c \
	../src/request.c \
	../src/sql_builder.c \
	../src/table_schema.c \
	../src/utils.c \
	../src/worker_pool.c \
	../src/worker.c \
	../src/metrics.c
	
COBJS:=$(SRCS:.c=.o)

all:$(BIN)

$(COBJS) : %.o: %.c
	$(CC) -c $< -o $@ $(INCS)

$(BIN):$(COBJS)
	$(CC) -o $(BIN) $(COBJS) $(STATCI_LIBS) $(SHARED_LIBS) $(CFLAGS)

clean:
	rm $(BIN) $(COBJS)