// xscrubber.bpf.c
#include "vmlinux.h"
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

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

struct rx_metadata {
	__u64 timestamp;
};

/* This kfunc is available when the attached driver advertises the
 * NETDEV_XDP_RX_METADATA_TIMESTAMP capability. */
extern int bpf_xdp_metadata_rx_timestamp(const struct xdp_md *ctx,
						 __u64 *timestamp) __ksym;

#define ETH_P_IP	0x0800		/* Internet Protocol packet	*/

static __always_inline int redirect_packet(struct xdp_md *ctx) {
    __u32 q_idx = ctx->rx_queue_index;

    return bpf_redirect_map(&SOCKETS, q_idx, XDP_PASS);
}

SEC("xdp")
int xdp_dispatch(struct xdp_md *ctx) {
	__u32 q_idx = ctx->rx_queue_index;
	struct rx_metadata *metadata;
	void *data;

	/* Reserve the timestamp in the UMEM headroom immediately before the
	 * packet data. The AF_XDP descriptor still points at packet data. */
	if (bpf_xdp_adjust_meta(ctx, -(int)sizeof(*metadata)) < 0)
		return XDP_ABORTED;

	data = (void *)(long)ctx->data;
	metadata = (void *)(long)ctx->data_meta;
	if ((void *)(metadata + 1) > data)
		return XDP_ABORTED;

	metadata->timestamp = 0;
	(void)bpf_xdp_metadata_rx_timestamp(ctx, &metadata->timestamp);

    // bpf_printk("Packet on i/f %u, queue %u\n", if_idx, q_idx);
    
    //__u32 *fd= bpf_map_lookup_elem(&SOCKETS, &q_idx);

    // Redirect to the specific socket at that index
    // Note: If the socket isn't bound (no entry at final_index),
    // this acts like XDP_PASS (unless XDP_ABORTED flag is used)
    //bpf_printk("redirecting to AF_XDP, fd= %u\n", fd);
    return redirect_packet(ctx);
}

/* Used when the kernel or driver cannot load the metadata kfunc. */
SEC("xdp")
int xdp_dispatch_fallback(struct xdp_md *ctx) {
    return redirect_packet(ctx);
}
/*
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
