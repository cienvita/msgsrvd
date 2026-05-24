# msgsrvd design notes

Working notes on internals not covered by the README. Add sections as
the pieces land.

## Open topics

- WAL layout and fsync policy
- Replication: prepare/commit on 2+1, ACK with WAL sequence
- Event loop on top of io_uring
- Connection-arena sizing and per-conn buffer limits
