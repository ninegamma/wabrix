# this is how to generate vmlinux.h

bpftool btf dump file /sys/kernel/btf/vmlinux format c > vmlinux.h