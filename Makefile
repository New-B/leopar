# Makefile for LeoPar prototype

CC = gcc
CFLAGS = -Iinclude -Wall -g
LDFLAGS = -lpthread

SRC = src/leopar_runtime.c src/log.c
OBJ = $(SRC:.c=.o)

all: apps/vec_add tests/test_threads

# Build applications
apps/%: apps/%.c $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

# Build tests
tests/%: tests/%.c $(OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(OBJ) apps/* tests/* logs/*

.PHONY: all clean
