CC      := gcc
CFLAGS  := -std=c99 -Wall -Wextra -Wpedantic -Werror

all: msgsrvd

msgsrvd: src/server/main.c
	$(CC) $(CFLAGS) -Isrc -o $@ $<

debug: CFLAGS += -O0 -g -DMSGSRVD_DEBUG
debug: msgsrvd

clean:
	rm -f msgsrvd

.PHONY: all debug clean
