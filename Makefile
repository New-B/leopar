# Makefile for LeoPar runtime (UCX TCP bootstrap + dispatcher + logging)
# Pure-API demos only include leopar.h

# ===== Toolchain & UCX path =====
CC        := gcc
# Default UCX_HOME (can be overridden in `make UCX_HOME=/path/to/ucx`)
UCX_HOME  := /usr

INCDIRS   := -Iinclude -I$(UCX_HOME)/include
LIBDIRS   := -L$(UCX_HOME)/lib

CFLAGS    := -Wall -g $(INCDIRS)
# -rdynamic makes main binary symbols visible to dlsym()
LDFLAGS   := $(LIBDIRS) -lucp -lucs -ldl -lpthread -rdynamic
# If UCX is in a non-standard runtime path, uncomment:
# LDFLAGS  += -Wl,-rpath,$(UCX_HOME)/lib

# Directories
SRC_DIR  := src
TEST_DIR := tests
BIN_DIR  := bin
LOG_DIR  := logs

# ===== Public/Private headers (for deps only) =====
HEADERS  := include/leopar.h \
            include/context.h \
            include/proto.h \
            include/tcp.h \
            include/ucx.h \
            include/dispatcher.h \
            include/tid.h \
            include/functable.h \
            include/threadtable.h \
            include/scheduler.h \
            include/log.h

# ===== Runtime sources =====
SRCS     := $(SRC_DIR)/leopar.c \
            $(SRC_DIR)/context.c \
            $(SRC_DIR)/tcp.c \
            $(SRC_DIR)/ucx.c \
            $(SRC_DIR)/dispatcher.c \
            $(SRC_DIR)/functable.c \
            $(SRC_DIR)/threadtable.c \
            $(SRC_DIR)/scheduler.c \
            $(SRC_DIR)/log.c 

OBJS     := $(SRCS:.c=.o)

# Demo programs (add more demos here)
DEMOS    := leoparDemo_create_join \
            leopar_api_multi_nodes_demo #\
            leopar_barrier_demo \
            leopar_mutex_demo

DEMO_SRCS := $(addprefix $(TEST_DIR)/,$(addsuffix .c,$(DEMOS)))
DEMO_OBJS := $(DEMO_SRCS:.c=.o)
DEMO_BINS := $(addprefix $(BIN_DIR)/,$(DEMOS))

# # Test program
# TEST_SRC := $(TEST_DIR)/leoparDemo_ucx_log.c $(TEST_DIR)/leoparDemo_create_join.c
# TEST_OBJ := $(TEST_SRC:.c=.o)
# TARGET   := $(BIN_DIR)/leoparDemo_ucx_log $(BIN_DIR)leoparDemo_create_join


# # Rules
# all: $(TARGET)

# Default target: build all demos
all: $(BIN_DIR) $(LOG_DIR) $(DEMO_BINS)

# ===== Link rules for demos =====
$(BIN_DIR)/%: $(TEST_DIR)/%.o $(OBJS) | $(BIN_DIR)
	$(CC) -o $@ $^ $(LDFLAGS)

# ===== Compile rules =====
$(SRC_DIR)/%.o: $(SRC_DIR)/%.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

$(TEST_DIR)/%.o: $(TEST_DIR)/%.c $(HEADERS)
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(LOG_DIR):
	mkdir -p $(LOG_DIR)

clean:
	rm -f $(SRC_DIR)/*.o $(TEST_DIR)/*.o
	rm -f $(DEMO_BINS)

.PHONY: all clean














# # Makefile for LeoPar prototype

# CC = gcc
# CFLAGS = -Iinclude -I/usr/lib/x86_64-linux-gnu/openmpi/include -Wall -g
# LDFLAGS = -lpthread -lmpi -lucp -lucs

# # RC = src/leopar_runtime.c src/log.c src/comm_ucx.c apps/vec_add.c tests/test_threads.c tests/init_agent.c
# # OBJ = $(SRC:.c=.o)
# # Source files
# SRC = src/leopar_runtime.c src/log.c src/ucx_tcp.c
# OBJ = $(SRC:.c=.o)

# # Applications and tests
# # APPS = apps/vec_add
# TESTS = tests/init_agent_tcp_ring

# # All executables
# all: $(APPS) $(TESTS)

# # all: apps/vec_add tests/test_threads tests/init_agent

# # Build applications
# # apps/vec_add: apps/vec_add.c $(OBJ)
# # 	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# # Build tests
# # tests/test_threads: tests/test_threads.c $(OBJ)
# # 	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# tests/init_agent_tcp_ring: tests/init_agent_tcp_ring.c $(OBJ)
# 	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# # Compile common source files
# %.o: %.c
# 	$(CC) $(CFLAGS) -c -o $@ $<

# # Clean up
# clean:
# 	rm -f $(COMMON_OBJ) logs/* src/*.o

# .PHONY: all clean