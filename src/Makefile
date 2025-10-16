# ===== src/Makefile =====
# Build the LeoPar runtime as a static library: ../src/libleopar.a

# Toolchain
CC        := gcc
AR        := ar
RANLIB    := ranlib
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
else
  UCX_LIBDIR := $(shell if [ -d "$(UCX_HOME)/lib64" ]; then echo "$(UCX_HOME)/lib64"; else echo "$(UCX_HOME)/lib"; fi)
  INCDIRS    := -I../include -I$(UCX_HOME)/include
endif

CFLAGS := -Wall -g -D_GNU_SOURCE -std=c11 $(INCDIRS)
CXXFLAGS  := -O2 -g -fPIC -Wall -Wextra -std=c++17 -I../include 
DSM_ROOT := $(PWD)/../../farm
DSM_INC  := -I$(DSM_ROOT)/include #-I$(DSM_ROOT)/lib/libcuckoo/src

# Sources in this directory
SRCS := leopar.c context.c tcp.c ucx.c dispatcher.c functable.c threadtable.c \
        scheduler.c sync.c query.c ctrl.c log.c dsm_c_api.cpp
OBJS := $(SRCS:.c=.o)
OBJS := $(OBJS:.cpp=.o)

# Output library (stays in src/)
LIB  := libleopar.a

all: $(LIB)

$(LIB): $(OBJS)
	$(AR) rcs $@ $^
	$(RANLIB) $@

%.o: %.c ../include/*.h
	$(CC) $(CFLAGS) -c $< -o $@

%.o: %.cpp ../include/*.h
	$(CXX) $(CXXFLAGS) $(DSM_INC) -c $< -o $@

clean:
	rm -f $(OBJS) $(LIB)

.PHONY: all clean
