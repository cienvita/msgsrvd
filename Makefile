CC      := gcc
CFLAGS  := -std=c99 -Wall -Wextra -Wpedantic -Werror

all: msgsrvd

msgsrvd: src/server/main.c
	$(CC) $(CFLAGS) -Isrc -o $@ $<

clean:
	rm -f msgsrvd

.PHONY: all clean
