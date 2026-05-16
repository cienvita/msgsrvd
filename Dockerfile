# syntax=docker/dockerfile:1

# Build the freestanding x86-64 binary.
FROM gcc:14-bookworm AS build
WORKDIR /src
COPY Makefile ./
COPY src ./src
RUN make tiny

# Ship it in scratch. No libc, no shell, no /lib. Just the binary.
FROM scratch
COPY --from=build /src/tiny /tiny
ENTRYPOINT ["/tiny"]
