// ipblower.bpf.c
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
/*
struct block_info {
    __u32 reason_code;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __type(key, __u32);
    __type(value, struct block_info);
    __uint(max_entries, 10000);
} BLOCKLIST SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __type(key, __u32);
    __type(value, __u32);
    __uint(max_entries, 64);
} SOCKETS SEC(".maps");

#define ETH_P_IP	0x0800
*/

SEC("xdp")
int xdp_dummy_prog(struct xdp_md *ctx) {
    // Let all incoming traffic proceed to the Linux kernel normally
    return XDP_PASS;
}


/*
SEC("xdp")
int xdp_dispatch(struct xdp_md *ctx) {
    __u32 q_idx = ctx->rx_queue_index;
    __u32 if_idx = ctx->ingress_ifindex;

    bpf_printk("Packet on i/f %u, queue %u\n", if_idx, q_idx);
    
    __u32 *fd= bpf_map_lookup_elem(&SOCKETS, &q_idx);

    // Step 3: Redirect to the specific socket at that index
    // Note: If the socket isn't bound (no entry at final_index),
    // this acts like XDP_PASS (unless XDP_ABORTED flag is used)
    bpf_printk("redirecting to AF_XDP, fd= %u\n", fd);
    return bpf_redirect_map(&SOCKETS, q_idx, XDP_PASS);


}

SEC("xdp")
int xscrubber(struct xdp_md *ctx)
{
    void *data_end = (void *)(long)ctx->data_end;
    void *data = (void *)(long)ctx->data;
    __u32 qid = ctx->rx_queue_index;

    // Parse Ethernet header
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;

    if (eth->h_proto != __bpf_constant_htons(ETH_P_IP))
        return XDP_PASS;

    // Parse IP header
    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end)
        return XDP_PASS;

    // Check blocklist
    __u32 src_ip = ip->saddr;
    struct block_info *info = bpf_map_lookup_elem(&BLOCKLIST, &src_ip);
    if (info) {
        bpf_printk("Blocking IP, reason: %d", info->reason_code);
        return XDP_DROP;
    }

    // Log packet
    bpf_printk("Queue %d: Redirecting packet", qid);

    // Redirect to AF_XDP socket
    return bpf_redirect_map(&SOCKETS, qid, XDP_PASS);
}
*/


char _license[] SEC("license") = "GPL";