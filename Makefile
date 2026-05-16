CC      := gcc
CFLAGS  := -std=c99 -Wall -Wextra -Wpedantic -Werror
STRIP   := strip

# Size-optimised, no libc, no CRT, no startup files.
# Drops unused sections, build-id, unwind tables, comment notes.
TINY_CFLAGS := -std=c99 -Wall -Wextra -Wpedantic -Werror \
               -Os -ffunction-sections -fdata-sections \
               -fno-asynchronous-unwind-tables -fno-unwind-tables \
               -fno-stack-protector -fno-pie -fno-builtin
TINY_LDFLAGS := -nostdlib -static -no-pie \
                -Wl,--gc-sections -Wl,--build-id=none \
                -Wl,-z,noseparate-code

all: msgsrvd tiny tinye

msgsrvd: src/server/main.c
	$(CC) $(CFLAGS) -Isrc -o $@ $<

debug: CFLAGS += -O0 -g -DMSGSRVD_DEBUG
debug: msgsrvd

tiny tinye: %: src/server/%.c
	$(CC) $(TINY_CFLAGS) $(TINY_LDFLAGS) -Isrc -o $@ $<
	$(STRIP) --strip-all -R .comment -R .note.gnu.property $@

clean:
	rm -f msgsrvd tiny tinye

.PHONY: all debug clean
