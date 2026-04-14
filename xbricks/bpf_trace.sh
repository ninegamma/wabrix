#!/bin/bash
sudo bpftrace -e 'tracepoint:xdp:xdp_redirect* { printf("Result: %d\n", args->err); }'
