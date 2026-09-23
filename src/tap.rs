//! Peering TAP source: read the gossip stream a node on this box ALREADY receives, instead of
//! opening a second subscription to the peering service.
//!
//! Why: a dialed collector next to a node doubles the service's egress to the box (the node's
//! stream plus the collector's). The tap opens no connection and sends nothing: it copies the
//! packets the kernel already delivered to the node's socket (read-only AF_PACKET capture with a
//! kernel filter for exactly one peer ip:port), rebuilds the in-order TCP byte stream, and hands it
//! to the same frame parser and round assembly the dialed path uses.
//!
//! Timing: each segment carries the kernel receive timestamp (SO_TIMESTAMPNS), i.e. when the
//! bytes reached this box, not when a userland reader got to them.
//!
//! Loss is never guessed over: a sequence hole that is not filled within [`REORDER_WAIT`] (capture
//! buffer overrun, a missed segment) drops the partial stream and the reader resynchronises on a
//! frame boundary; rounds that could not complete are counted as gaps by the assembly deadline.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

/// A hole in the sequence space older than this is treated as lost.
pub const REORDER_WAIT: Duration = Duration::from_millis(250);
/// Bound on out-of-order bytes held per flow while waiting for a hole to fill.
const MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

const TCP_FIN: u8 = 0x01;
const TCP_SYN: u8 = 0x02;
const TCP_RST: u8 = 0x04;

/// One TCP segment from the tapped peer, as captured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    /// Local (node-side) port: one tapped connection per value.
    pub dst_port: u16,
    pub seq: u32,
    pub flags: u8,
    pub payload: Vec<u8>,
    /// Kernel receive time, ns since the Unix epoch.
    pub wall_ns: u64,
}

/// Parse an IPv4 packet (network header first, as AF_PACKET SOCK_DGRAM delivers it) into a
/// segment when it is TCP from `src:src_port`. Anything else is None.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn parse_ipv4_tcp(
    packet: &[u8],
    src: Ipv4Addr,
    src_port: u16,
    wall_ns: u64,
) -> Option<Segment> {
    if packet.len() < 20 || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    let total = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    // Fragments never carry a whole TCP header we can trust; the filter excludes nothing else.
    let frag = u16::from_be_bytes([packet[6], packet[7]]);
    if ihl < 20 || packet[9] != 6 || frag & 0x3fff != 0 {
        return None;
    }
    if Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]) != src {
        return None;
    }
    // GRO can hand us a coalesced packet whose IP total length is 0 or stale; trust the capture.
    let end = if total >= ihl + 20 && total <= packet.len() {
        total
    } else {
        packet.len()
    };
    let tcp = packet.get(ihl..end)?;
    if tcp.len() < 20 || u16::from_be_bytes([tcp[0], tcp[1]]) != src_port {
        return None;
    }
    let data_offset = usize::from(tcp[12] >> 4) * 4;
    if data_offset < 20 || data_offset > tcp.len() {
        return None;
    }
    Some(Segment {
        dst_port: u16::from_be_bytes([tcp[2], tcp[3]]),
        seq: u32::from_be_bytes([tcp[4], tcp[5], tcp[6], tcp[7]]),
        flags: tcp[13],
        payload: tcp[data_offset..].to_vec(),
        wall_ns,
    })
}

/// What the reassembler hands on for one segment.
#[derive(Debug, PartialEq, Eq)]
pub enum Delivery {
    /// In-order bytes, with the receive time of the segment that completed them.
    Bytes { data: Vec<u8>, wall_ns: u64 },
    /// The byte stream restarted or lost bytes: discard any partial frame and resynchronise.
    Reset { reason: &'static str },
}

/// In-order TCP byte stream for one tapped connection. Retransmits and duplicate copies are
/// dropped; out-of-order segments wait up to [`REORDER_WAIT`] for the hole before them.
#[derive(Debug, Default)]
pub struct Reassembler {
    next: Option<u32>,
    /// seq -> (payload, first seen)
    pending: BTreeMap<u32, (Vec<u8>, u64, Instant)>,
    pending_bytes: usize,
}

/// Signed distance from `b` to `a` in 32-bit sequence space.
fn seq_diff(a: u32, b: u32) -> i32 {
    a.wrapping_sub(b) as i32
}

impl Reassembler {
    pub fn push(&mut self, seg: Segment, now: Instant) -> Vec<Delivery> {
        let mut out = Vec::new();
        if seg.flags & TCP_SYN != 0 {
            // A new connection on this port: whatever we held belongs to the old one.
            if self.next.is_some() {
                out.push(Delivery::Reset {
                    reason: "new connection (SYN)",
                });
            }
            self.clear();
            self.next = Some(seg.seq.wrapping_add(1));
            if !seg.payload.is_empty() {
                self.take(seg.seq.wrapping_add(1), seg.payload, seg.wall_ns, &mut out);
            }
            return out;
        }
        if seg.flags & TCP_RST != 0 {
            if self.next.is_some() {
                out.push(Delivery::Reset {
                    reason: "connection reset (RST)",
                });
            }
            self.clear();
            return out;
        }
        if seg.payload.is_empty() {
            if seg.flags & TCP_FIN != 0 && self.next.is_some() {
                out.push(Delivery::Reset {
                    reason: "connection closed (FIN)",
                });
                self.clear();
            }
            return out;
        }
        // Joined mid-stream: start at the first byte we see; the frame reader resynchronises.
        self.next.get_or_insert(seg.seq);
        self.take(seg.seq, seg.payload, seg.wall_ns, &mut out);
        self.expire(now, &mut out);
        out
    }

    /// Called periodically without traffic so a hole cannot hold bytes forever.
    pub fn tick(&mut self, now: Instant) -> Vec<Delivery> {
        let mut out = Vec::new();
        self.expire(now, &mut out);
        out
    }

    fn clear(&mut self) {
        self.next = None;
        self.pending.clear();
        self.pending_bytes = 0;
    }

    fn take(&mut self, seq: u32, mut payload: Vec<u8>, wall_ns: u64, out: &mut Vec<Delivery>) {
        let Some(mut next) = self.next else { return };
        let d = seq_diff(seq, next);
        if d < 0 {
            // Retransmit or overlap: keep only bytes past `next`.
            let skip = d.unsigned_abs() as usize;
            if skip >= payload.len() {
                return;
            }
            payload.drain(..skip);
        } else if d > 0 {
            if self.pending_bytes + payload.len() > MAX_PENDING_BYTES {
                // Too much held behind one hole: give up on it and restart at this segment.
                out.push(Delivery::Reset {
                    reason: "reorder buffer full",
                });
                self.pending.clear();
                self.pending_bytes = 0;
                next = seq;
            } else {
                self.pending_bytes += payload.len();
                self.pending
                    .entry(seq)
                    .or_insert((payload, wall_ns, Instant::now()));
                return;
            }
        }
        next = next.wrapping_add(payload.len() as u32);
        let mut data = payload;
        let mut last_wall = wall_ns;
        // Pull every pending segment the new bytes made contiguous.
        loop {
            let Some((&s, _)) = self.pending.iter().find(|(s, _)| seq_diff(**s, next) <= 0) else {
                break;
            };
            let (mut p, w, _) = self.pending.remove(&s).expect("present");
            self.pending_bytes -= p.len();
            let overlap = seq_diff(next, s);
            if overlap as usize >= p.len() {
                continue;
            }
            p.drain(..overlap as usize);
            next = next.wrapping_add(p.len() as u32);
            data.extend_from_slice(&p);
            last_wall = last_wall.max(w);
        }
        self.next = Some(next);
        out.push(Delivery::Bytes {
            data,
            wall_ns: last_wall,
        });
    }

    fn expire(&mut self, now: Instant, out: &mut Vec<Delivery>) {
        let Some(oldest) = self.pending.values().map(|(_, _, t)| *t).min() else {
            return;
        };
        if now.saturating_duration_since(oldest) < REORDER_WAIT {
            return;
        }
        // The hole never filled: those bytes are lost to us. Jump to the earliest held segment.
        out.push(Delivery::Reset {
            reason: "sequence hole not filled (capture loss)",
        });
        let first = *self
            .pending
            .keys()
            .min_by_key(|s| seq_diff(**s, self.next.unwrap_or(**s)))
            .expect("non-empty");
        let (payload, wall, _) = self.pending.remove(&first).expect("present");
        self.pending_bytes -= payload.len();
        self.next = Some(first);
        self.take(first, payload, wall, out);
    }
}

/// Classic-BPF program for AF_PACKET SOCK_DGRAM (data starts at the IP header):
/// accept IPv4/TCP with source `src:src_port`, drop everything else in the kernel.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub fn bpf_program(src: Ipv4Addr, src_port: u16) -> [(u16, u8, u8, u32); 9] {
    let src_k = u32::from(src);
    [
        (0x20, 0, 0, 12),                  // ld  [12]            ip src
        (0x15, 0, 6, src_k),               // jeq #src   else drop
        (0x30, 0, 0, 9),                   // ldb [9]             protocol
        (0x15, 0, 4, 6),                   // jeq #tcp   else drop
        (0xb1, 0, 0, 0),                   // ldxb 4*([0]&0xf)    ip header length
        (0x48, 0, 0, 0),                   // ldh [x+0]           tcp src port
        (0x15, 0, 1, u32::from(src_port)), // jeq #port else drop
        (0x06, 0, 0, 0x0004_0000),         // ret 256 KiB (whole packet)
        (0x06, 0, 0, 0),                   // ret 0 (drop)
    ]
}

#[cfg(target_os = "linux")]
pub use linux::spawn_capture;

#[cfg(not(target_os = "linux"))]
pub fn spawn_capture(
    _src: Ipv4Addr,
    _src_port: u16,
    _tx: tokio::sync::mpsc::Sender<Segment>,
) -> anyhow::Result<std::thread::JoinHandle<()>> {
    anyhow::bail!("peering tap needs Linux AF_PACKET capture")
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Segment, bpf_program, parse_ipv4_tcp};
    use anyhow::{Context, Result};
    use std::net::Ipv4Addr;
    use tracing::warn;

    const ETH_P_IP: u16 = 0x0800;
    const PACKET_OUTGOING: u8 = 4;
    const ARPHRD_LOOPBACK: u16 = 772;

    /// Open a read-only capture socket filtered in the kernel to `src:src_port` and forward each
    /// segment on `tx` from a dedicated thread. Needs CAP_NET_RAW; sends nothing.
    pub fn spawn_capture(
        src: Ipv4Addr,
        src_port: u16,
        tx: tokio::sync::mpsc::Sender<Segment>,
    ) -> Result<std::thread::JoinHandle<()>> {
        // SAFETY: plain libc socket calls on a descriptor this function owns.
        let fd = unsafe {
            libc::socket(
                libc::AF_PACKET,
                libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
                i32::from(ETH_P_IP.to_be()),
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error())
                .context("open AF_PACKET capture (needs CAP_NET_RAW)");
        }
        let prog = bpf_program(src, src_port);
        let filter: Vec<libc::sock_filter> = prog
            .iter()
            .map(|&(code, jt, jf, k)| libc::sock_filter { code, jt, jf, k })
            .collect();
        let fprog = libc::sock_fprog {
            len: filter.len() as u16,
            filter: filter.as_ptr() as *mut _,
        };
        let one: libc::c_int = 1;
        let rcvbuf: libc::c_int = 32 * 1024 * 1024;
        // SAFETY: option pointers/lengths match the option types.
        unsafe {
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_ATTACH_FILTER,
                (&fprog as *const libc::sock_fprog).cast(),
                std::mem::size_of::<libc::sock_fprog>() as u32,
            ) != 0
            {
                let e = std::io::Error::last_os_error();
                libc::close(fd);
                return Err(e).context("attach capture filter");
            }
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUFFORCE,
                (&rcvbuf as *const libc::c_int).cast(),
                4,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&rcvbuf as *const libc::c_int).cast(),
                4,
            );
            if libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_TIMESTAMPNS,
                (&one as *const libc::c_int).cast(),
                4,
            ) != 0
            {
                let e = std::io::Error::last_os_error();
                libc::close(fd);
                return Err(e).context("enable kernel receive timestamps");
            }
        }
        let handle = std::thread::Builder::new()
            .name("peering-tap".into())
            .spawn(move || {
                let mut buf = vec![0u8; 256 * 1024];
                let mut ctrl = [0u8; 256];
                loop {
                    let mut addr: libc::sockaddr_ll = unsafe { std::mem::zeroed() };
                    let mut iov = libc::iovec {
                        iov_base: buf.as_mut_ptr().cast(),
                        iov_len: buf.len(),
                    };
                    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
                    msg.msg_name = (&mut addr as *mut libc::sockaddr_ll).cast();
                    msg.msg_namelen = std::mem::size_of::<libc::sockaddr_ll>() as u32;
                    msg.msg_iov = &mut iov;
                    msg.msg_iovlen = 1;
                    msg.msg_control = ctrl.as_mut_ptr().cast();
                    msg.msg_controllen = ctrl.len() as _;
                    // SAFETY: msg points at live buffers sized above.
                    let n = unsafe { libc::recvmsg(fd, &mut msg, 0) };
                    if n < 0 {
                        let e = std::io::Error::last_os_error();
                        if e.kind() == std::io::ErrorKind::Interrupted {
                            continue;
                        }
                        warn!(?e, "peering tap capture failed");
                        break;
                    }
                    // Loopback shows each packet twice (out + in): keep the receive copy there.
                    // Elsewhere keep outgoing copies too: a node in a container behind a bridge
                    // receives what the host FORWARDS, which the host sees only as outgoing (the
                    // bridge + veth duplicates are dropped by the reassembler as retransmits).
                    if addr.sll_pkttype == PACKET_OUTGOING && addr.sll_hatype == ARPHRD_LOOPBACK {
                        continue;
                    }
                    let wall_ns = kernel_timestamp(&msg).unwrap_or_else(now_ns);
                    if let Some(seg) = parse_ipv4_tcp(&buf[..n as usize], src, src_port, wall_ns)
                        && tx.blocking_send(seg).is_err()
                    {
                        break;
                    }
                }
                // SAFETY: fd is owned by this thread from here on.
                unsafe { libc::close(fd) };
            })
            .context("spawn peering tap thread")?;
        Ok(handle)
    }

    fn kernel_timestamp(msg: &libc::msghdr) -> Option<u64> {
        // SAFETY: walking the control buffer recvmsg filled, with libc's own CMSG helpers.
        unsafe {
            let mut cmsg = libc::CMSG_FIRSTHDR(msg);
            while !cmsg.is_null() {
                if (*cmsg).cmsg_level == libc::SOL_SOCKET
                    && (*cmsg).cmsg_type == libc::SCM_TIMESTAMPNS
                {
                    let ts: libc::timespec = std::ptr::read_unaligned(libc::CMSG_DATA(cmsg).cast());
                    return Some(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64);
                }
                cmsg = libc::CMSG_NXTHDR(msg, cmsg);
            }
        }
        None
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(port: u16, seq: u32, flags: u8, payload: &[u8]) -> Segment {
        Segment {
            dst_port: port,
            seq,
            flags,
            payload: payload.to_vec(),
            wall_ns: u64::from(seq),
        }
    }

    fn bytes(out: &[Delivery]) -> Vec<u8> {
        out.iter()
            .filter_map(|d| match d {
                Delivery::Bytes { data, .. } => Some(data.clone()),
                Delivery::Reset { .. } => None,
            })
            .flatten()
            .collect()
    }

    fn packet(
        src: [u8; 4],
        sport: u16,
        dport: u16,
        seq: u32,
        flags: u8,
        payload: &[u8],
    ) -> Vec<u8> {
        let mut tcp = vec![0u8; 20];
        tcp[0..2].copy_from_slice(&sport.to_be_bytes());
        tcp[2..4].copy_from_slice(&dport.to_be_bytes());
        tcp[4..8].copy_from_slice(&seq.to_be_bytes());
        tcp[12] = 5 << 4;
        tcp[13] = flags;
        tcp.extend_from_slice(payload);
        let mut ip = vec![0u8; 20];
        ip[0] = 0x45;
        ip[2..4].copy_from_slice(&((20 + tcp.len()) as u16).to_be_bytes());
        ip[9] = 6;
        ip[12..16].copy_from_slice(&src);
        ip[16..20].copy_from_slice(&[10, 0, 0, 1]);
        ip.extend_from_slice(&tcp);
        ip
    }

    #[test]
    fn parses_only_the_tapped_peer() {
        let peer = Ipv4Addr::new(175, 41, 198, 192);
        let p = packet([175, 41, 198, 192], 4001, 50123, 7, 0x18, b"abc");
        let s = parse_ipv4_tcp(&p, peer, 4001, 9).expect("tcp from the peer");
        assert_eq!(
            (s.dst_port, s.seq, s.payload.as_slice(), s.wall_ns),
            (50123, 7, &b"abc"[..], 9)
        );
        assert!(
            parse_ipv4_tcp(&p, Ipv4Addr::new(1, 2, 3, 4), 4001, 0).is_none(),
            "other source"
        );
        assert!(
            parse_ipv4_tcp(&p, peer, 4002, 0).is_none(),
            "other port (4002 backfill is not the stream)"
        );
        let mut udp = p.clone();
        udp[9] = 17;
        assert!(parse_ipv4_tcp(&udp, peer, 4001, 0).is_none());
    }

    #[test]
    fn in_order_retransmit_and_overlap() {
        let now = Instant::now();
        let mut r = Reassembler::default();
        let mut out = r.push(seg(1, 100, 0x18, b"hello"), now);
        out.extend(r.push(seg(1, 100, 0x18, b"hello"), now)); // exact retransmit
        out.extend(r.push(seg(1, 103, 0x18, b"lo wor"), now)); // overlap
        out.extend(r.push(seg(1, 109, 0x18, b"ld"), now));
        assert_eq!(bytes(&out), b"hello world");
        assert!(out.iter().all(|d| matches!(d, Delivery::Bytes { .. })));
    }

    #[test]
    fn out_of_order_segments_wait_for_the_hole() {
        let now = Instant::now();
        let mut r = Reassembler::default();
        let mut out = r.push(seg(1, 0, 0x18, b"aa"), now);
        out.extend(r.push(seg(1, 4, 0x18, b"cc"), now));
        assert_eq!(bytes(&out), b"aa", "cc waits for the hole");
        out.extend(r.push(seg(1, 2, 0x18, b"bb"), now));
        assert_eq!(bytes(&out), b"aabbcc");
    }

    #[test]
    fn an_unfilled_hole_is_a_reset_never_a_splice() {
        let t0 = Instant::now();
        let mut r = Reassembler::default();
        let mut out = r.push(seg(1, 0, 0x18, b"aa"), t0);
        out.extend(r.push(seg(1, 10, 0x18, b"zz"), t0));
        assert!(r.tick(t0 + Duration::from_millis(100)).is_empty());
        let late = r.tick(t0 + REORDER_WAIT + Duration::from_millis(10));
        assert!(matches!(late[0], Delivery::Reset { .. }), "{late:?}");
        assert_eq!(
            bytes(&late),
            b"zz",
            "the stream resumes after the hole, flagged"
        );
    }

    #[test]
    fn syn_rst_fin_restart_the_stream() {
        let now = Instant::now();
        let mut r = Reassembler::default();
        r.push(seg(1, 0, 0x18, b"old"), now);
        let out = r.push(seg(1, 5000, TCP_SYN, b""), now);
        assert!(matches!(out[0], Delivery::Reset { .. }));
        let out = r.push(seg(1, 5001, 0x18, b"new"), now);
        assert_eq!(bytes(&out), b"new");
        assert!(matches!(
            r.push(seg(1, 0, TCP_RST, b""), now)[0],
            Delivery::Reset { .. }
        ));
    }

    #[test]
    fn sequence_wraparound() {
        let now = Instant::now();
        let mut r = Reassembler::default();
        let mut out = r.push(seg(1, u32::MAX - 1, 0x18, b"ab"), now);
        out.extend(r.push(seg(1, 0, 0x18, b"cd"), now));
        assert_eq!(bytes(&out), b"abcd");
    }

    #[test]
    fn bpf_program_jumps_land_on_the_drop() {
        let prog = bpf_program(Ipv4Addr::new(175, 41, 198, 192), 4001);
        for (i, &(code, jt, jf, _)) in prog.iter().enumerate() {
            if code == 0x15 {
                assert_eq!(i + 1 + usize::from(jt), i + 1, "match falls through");
                assert_eq!(
                    i + 1 + usize::from(jf),
                    prog.len() - 1,
                    "mismatch jumps to ret 0"
                );
            }
        }
        assert_eq!(prog[1].3, u32::from_be_bytes([175, 41, 198, 192]));
        assert_eq!(prog[6].3, 4001);
    }
}
