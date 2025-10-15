# ===== tests/Makefile =====
# Build test binaries, link against ../src/libleopar.a, UCX, and GAM (C++)

# Toolchain
CC        := gcc
CXX       := g++

# UCX
UCX_HOME  ?= /usr/local/ucx-1.16.0
HAVE_PKG_UCX := $(shell PKG_CONFIG_PATH=$(UCX_HOME)/lib/pkgconfig:$(PKG_CONFIG_PATH) \
                   pkg-config --exists ucx && echo yes || echo no)

ifeq ($(HAVE_PKG_UCX),yes)
  UCX_CFLAGS := $(shell PKG_CONFIG_PATH=$(UCX_HOME)/lib/pkgconfig:$(PKG_CONFIG_PATH) pkg-config --cflags ucx)
  UCX_LIBS   := $(shell PKG_CONFIG_PATH=$(UCX_HOME)/lib/pkgconfig:$(PKG_CONFIG_PATH) pkg-config --libs ucx)
  UCX_LIBDIR := $(shell PKG_CONFIG_PATH=$(UCX_HOME)/lib/pkgconfig:$(PKG_CONFIG_PATH) pkg-config --variable=libdir ucx)
  INCDIRS    := -I../include $(UCX_CFLAGS)
  UCX_LINK   := $(UCX_LIBS)
else
  UCX_LIBDIR := $(shell if [ -d "$(UCX_HOME)/lib64" ]; then echo "$(UCX_HOME)/lib64"; else echo "$(UCX_HOME)/lib"; fi)
  INCDIRS    := -I../include -I$(UCX_HOME)/include
  UCX_LINK   := -L$(UCX_LIBDIR) -lucp -luct -lucs -lucm
endif

# GAM headers/libs (adjust paths if needed)
GAM_INC   ?= -I/users/NewB/gam/include
GAM_LIBS  ?= -L/users/NewB/gam/src -ldsm

CFLAGS  := -Wall -g -D_GNU_SOURCE -std=c11 $(INCDIRS) $(GAM_INC)
LDFLAGS := $(UCX_LINK) -ldl -lpthread -rdynamic -Wl,--no-as-needed -Wl,-rpath,$(UCX_LIBDIR) $(GAM_LIBS) -lm

# Where to drop binaries and logs
BIN_DIR := ../bin
LOG_DIR := ../logs

# Runtime static library from ../src
LEOPAR_LIB := ../src/libleopar.a

# Demo list (same as top-level)
DEMOS := leoparDemo_create_join \
         leopar_api_multi_nodes_demo \
         leopar_byval_demo \
         leopar_mb_create_join

DEMO_SRCS := $(addsuffix .c,$(DEMOS))
DEMO_OBJS := $(DEMO_SRCS:.c=.o)
DEMO_BINS := $(addprefix $(BIN_DIR)/,$(DEMOS))

all: $(BIN_DIR) $(LOG_DIR) $(DEMO_BINS)

$(BIN_DIR)/%: %.o $(LEOPAR_LIB) | $(BIN_DIR)
	# Link with C++ linker to satisfy GAM (C++) runtime
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.c ../include/*.h
	$(CC) $(CFLAGS) -c $< -o $@

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(LOG_DIR):
	mkdir -p $(LOG_DIR)

clean:
	rm -f $(DEMO_OBJS) $(DEMO_BINS)

.PHONY: all clean
