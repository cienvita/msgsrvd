CC      := gcc
CFLAGS  := -std=c99 -Wall -Wextra -Wpedantic -Werror
STRIP   := strip
BUILD   := build

# Size-optimised, no libc, no CRT, no startup files.
# Drops unused sections, build-id, unwind tables, comment notes.
TINY_CFLAGS := -std=c99 -Wall -Wextra -Wpedantic -Werror \
               -Os -ffunction-sections -fdata-sections \
               -fno-asynchronous-unwind-tables -fno-unwind-tables \
               -fno-stack-protector -fno-pie -fno-builtin
TINY_LDFLAGS := -nostdlib -static -no-pie \
                -Wl,--gc-sections -Wl,--build-id=none \
                -Wl,-z,noseparate-code

all: $(BUILD)/msgsrvd $(BUILD)/tiny $(BUILD)/tinye

msgsrvd: $(BUILD)/msgsrvd
tiny:    $(BUILD)/tiny
tinye:   $(BUILD)/tinye

$(BUILD)/msgsrvd: src/server/main.c | $(BUILD)
	$(CC) $(CFLAGS) -Isrc -o $@ $<

debug: CFLAGS += -O0 -g -DMSGSRVD_DEBUG
debug: $(BUILD)/msgsrvd

$(BUILD)/tiny $(BUILD)/tinye: $(BUILD)/%: src/server/%.c | $(BUILD)
	$(CC) $(TINY_CFLAGS) $(TINY_LDFLAGS) -Isrc -o $@ $<
	$(STRIP) --strip-all -R .comment -R .note.gnu.property $@

$(BUILD):
	mkdir -p $@

docker:
	docker build -t msgsrvd-tiny:latest .

clean:
	rm -rf $(BUILD)

.PHONY: all debug clean docker msgsrvd tiny tinye
