 sudo bpftrace -e '
kprobe:xsk_bind { printf("-> xsk_bind called\n"); }
kretprobe:xsk_bind { printf("<- xsk_bind returned: %d\n", retval); }
kprobe:xsk_release { printf("-> xsk_release called (Socket Destroyed!)\n"); }
kretprobe:xsk_release { printf("<- xsk_release finished\n"); '
