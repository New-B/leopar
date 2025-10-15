# ===== apps/Makefile =====
# Build app binaries (e.g., matrix_multiply), link ../src/libleopar.a, UCX, GAM

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

# GAM headers/libs (adjust if needed)
GAM_ROOT  ?= $(PWD)/../../gam
GAM_INC   ?= -I$(GAM_ROOT)/include
GAM_LIBDIR  ?= $(GAM_ROOT)/src
# CityHash static lib (adjust if needed)
CITYHASH_A  ?= $(GAM_ROOT)/lib/libcuckoo/cityhash-1.1.1/src/.libs/libcityhash.a

GAM_LIBS  ?= -L$(GAM_LIBDIR) -ldsm $(GAM_LIBDIR)/libgalloc.a -libverbs -lpthread $(CITYHASH_A) -lboost_thread -lboost_system

CFLAGS  := -Wall -g -D_GNU_SOURCE -std=c11 $(INCDIRS) $(GAM_INC)
LDFLAGS := -Wl,--start-group $(GAM_LIBS) -Wl,--end-group $(UCX_LINK) -ldl -lpthread -rdynamic -Wl,--no-as-needed -Wl,-rpath,$(UCX_LIBDIR)  -lm

BIN_DIR := ../bin
LOG_DIR := ../logs

LEOPAR_LIB := ../src/libleopar.a

# Apps list
APPS := matrix_multiply

APP_SRCS := $(addsuffix .c,$(APPS))
APP_OBJS := $(APP_SRCS:.c=.o)
APP_BINS := $(addprefix $(BIN_DIR)/,$(APPS))

all: $(BIN_DIR) $(LOG_DIR) $(APP_BINS)

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
	rm -f $(APP_OBJS) $(APP_BINS)

.PHONY: all clean
