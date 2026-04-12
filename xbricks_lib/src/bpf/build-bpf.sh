#!/bin/bash
# build-bpf.sh

clang -O2 -g -target bpf \
    -D__TARGET_ARCH_x86_64 \
    -I/usr/include/x86_64-linux-gnu \
    -c xscrubber.bpf.c -o xscrubber.bpf.o
