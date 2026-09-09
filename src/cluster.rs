//! Cluster module for Redis Cluster support.
//!
//! Implements the core cluster state management, including:
//! - 16384 hash slots ownership model
//! - CRC16 key hashing for slot routing
//! - CLUSTER INFO/NODES/SLOTS/KEYSLOT/MYID subcommands
//! - MOVED/ASK error responses for cross-node key access

use std::collections::HashMap;
use std::fmt;

/// Cluster node states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterNodeState {
    /// Node is reachable and serving slots
    Ok,
    /// Node is unreachable
    Fail,
    /// Node is reachable but not assigned any slots
    CantAssignSlots,
    /// Node is a replica waiting for failover
    Pfail,
}

impl fmt::Display for ClusterNodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterNodeState::Ok => write!(f, "ok"),
            ClusterNodeState::Fail => write!(f, "fail"),
            ClusterNodeState::CantAssignSlots => write!(f, "fail"),
            ClusterNodeState::Pfail => write!(f, "pfail"),
        }
    }
}

/// Represents a single node in the cluster
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Unique 40-character hex node ID
    pub id: String,
    /// Node IP address
    pub ip: String,
    /// Node port (cluster bus port = port + 10000)
    pub port: u16,
    /// Slots owned by this node (bitmap of 16384 slots)
    pub slots: [bool; 16384],
    /// Current node state
    pub state: ClusterNodeState,
    /// Node flags (myself, master, slave, etc.)
    pub flags: String,
    /// Link state (connected, disconnected)
    pub link_state: String,
}

impl ClusterNode {
    /// Create a new node with all slots assigned
    pub fn new_with_all_slots(id: String, ip: String, port: u16) -> Self {
        let mut node = Self {
            id,
            ip,
            port,
            slots: [false; 16384],
            state: ClusterNodeState::Ok,
            flags: "myself,master".to_string(),
            link_state: "connected".to_string(),
        };
        // Assign all slots
        for slot in node.slots.iter_mut() {
            *slot = true;
        }
        node
    }

    /// Count owned slots
    pub fn slot_count(&self) -> usize {
        self.slots.iter().filter(|&&s| s).count()
    }

    /// Get the minimum slot owned (for CLUSTER SLOTS response)
    pub fn min_slot(&self) -> Option<u16> {
        self.slots.iter().position(|&s| s).map(|i| i as u16)
    }

    /// Get the maximum slot owned (for CLUSTER SLOTS response)
    pub fn max_slot(&self) -> Option<u16> {
        self.slots.iter().rposition(|&s| s).map(|i| i as u16)
    }

    /// Check if a slot is owned by this node
    pub fn owns_slot(&self, slot: u16) -> bool {
        (slot as usize) < 16384 && self.slots[slot as usize]
    }
}

/// Main cluster state structure
pub struct ClusterState {
    /// This node's unique ID (40-char hex)
    pub node_id: String,
    /// This node's IP address
    pub node_ip: String,
    /// This node's port
    pub node_port: u16,
    /// Known nodes in the cluster (node_id -> ClusterNode)
    pub known_nodes: HashMap<String, ClusterNode>,
    /// Current cluster state
    pub state: ClusterNodeState,
    /// Whether cluster mode is enabled
    pub enabled: bool,
    /// Configuration epoch
    pub config_epoch: u64,
    /// Current epoch
    pub epoch: u64,
    /// Whether slots are being reassigned
    pub slots_reassigning: bool,
}

impl ClusterState {
    /// Create a new single-node cluster state (all slots belong to self)
    pub fn new_single_node(port: u16) -> Self {
        // Generate a random 40-char hex node ID
        let node_id = generate_node_id();
        let node_ip = "127.0.0.1".to_string();

        let self_node = ClusterNode::new_with_all_slots(node_id.clone(), node_ip.clone(), port);

        let mut known_nodes = HashMap::new();
        known_nodes.insert(node_id.clone(), self_node);

        Self {
            node_id: node_id.clone(),
            node_ip: node_ip.clone(),
            node_port: port,
            known_nodes,
            state: ClusterNodeState::Ok,
            enabled: true,
            config_epoch: 1,
            epoch: 1,
            slots_reassigning: false,
        }
    }

    /// Get the slot number for a given key using CRC16 hash
    pub fn key_slot(key: &[u8]) -> u16 {
        // Handle hash tags: if key contains {...}, hash only the content between braces
        if let Some(start) = key.iter().position(|&b| b == b'{') {
            if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
                if end > 0 {
                    let hash_key = &key[start + 1..start + 1 + end];
                    return crc16(hash_key) % 16384;
                }
            }
        }
        crc16(key) % 16384
    }

    /// Check if this node owns the slot for a given key
    pub fn owns_key(&self, key: &[u8]) -> bool {
        let slot = Self::key_slot(key);
        self.owns_slot(slot)
    }

    /// Check if this node owns a specific slot
    pub fn owns_slot(&self, slot: u16) -> bool {
        self.known_nodes
            .get(&self.node_id)
            .map(|n| n.owns_slot(slot))
            .unwrap_or(false)
    }

    /// Get the node that owns a specific slot
    pub fn slot_owner(&self, slot: u16) -> Option<&ClusterNode> {
        for node in self.known_nodes.values() {
            if node.owns_slot(slot) {
                return Some(node);
            }
        }
        None
    }

    /// Generate CLUSTER INFO response
    pub fn cluster_info(&self) -> String {
        let my_node = self.known_nodes.get(&self.node_id);
        let total_slots = self
            .known_nodes
            .values()
            .map(|n| n.slot_count())
            .sum::<usize>();
        let slots_assigned = self.known_nodes.values().any(|n| n.slot_count() > 0);

        let state_str = if self.state == ClusterNodeState::Ok {
            "ok"
        } else {
            "fail"
        };

        format!(
            "cluster_state:{}\r\n\
             cluster_slots:{}\r\n\
             cluster_slots_assigned:{}\r\n\
             cluster_slots_ok:{}\r\n\
             cluster_slots_pfail:{}\r\n\
             cluster_slots_fail:{}\r\n\
             cluster_known_nodes:{}\r\n\
             cluster_size:{}\r\n\
             cluster_current_epoch:{}\r\n\
             cluster_my_epoch:{}\r\n\
             cluster_stats-messages-sent:0\r\n\
             cluster_stats-messages-received:0\r\n\
             cluster_configs_bytes:0\r\n",
            state_str,
            16384,
            total_slots,
            if slots_assigned { total_slots } else { 0 },
            0,
            0,
            self.known_nodes.len(),
            self.known_nodes
                .values()
                .filter(|n| n.slot_count() > 0)
                .count(),
            self.epoch,
            self.config_epoch,
        )
    }

    /// Generate CLUSTER NODES response (Redis format)
    pub fn cluster_nodes(&self) -> String {
        let mut result = String::new();
        for (id, node) in &self.known_nodes {
            // Format: <id> <ip>:<port>@<cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot1>-<slot2>
            let flags = if node.id == self.node_id {
                &node.flags
            } else {
                "master"
            };

            // Get slot ranges
            let slot_ranges = get_slot_ranges(&node.slots);

            result.push_str(&format!(
                "{} {}:{}@{} {} - 0 0 {} {} {}\r\n",
                id,
                node.ip,
                node.port,
                node.port + 10000,
                flags,
                self.config_epoch,
                node.link_state,
                slot_ranges,
            ));
        }
        result
    }

    /// Generate CLUSTER SLOTS response
    pub fn cluster_slots(&self) -> Vec<(u16, u16, Vec<(String, u16)>)> {
        let mut slots_info = Vec::new();

        for node in self.known_nodes.values() {
            if let Some(min_slot) = node.min_slot() {
                if let Some(max_slot) = node.max_slot() {
                    slots_info.push((min_slot, max_slot, vec![(node.ip.clone(), node.port)]));
                }
            }
        }

        // Sort by start slot
        slots_info.sort_by_key(|s| s.0);
        slots_info
    }

    /// Generate CLUSTER MYID response
    pub fn cluster_myid(&self) -> &str {
        &self.node_id
    }

    /// Generate MOVED error response
    pub fn moved_error(&self, slot: u16) -> String {
        if let Some(owner) = self.slot_owner(slot) {
            format!("MOVED {} {}:{}", slot, owner.ip, owner.port)
        } else {
            format!(
                "MOVED {} {}:{}-{}",
                slot, "127.0.0.1", self.node_port, self.node_port
            )
        }
    }

    /// Generate ASK error response
    pub fn ask_error(&self, slot: u16) -> String {
        if let Some(owner) = self.slot_owner(slot) {
            format!("ASK {} {}:{}", slot, owner.ip, owner.port)
        } else {
            format!(
                "ASK {} {}:{}-{}",
                slot, "127.0.0.1", self.node_port, self.node_port
            )
        }
    }
}

/// Generate a random 40-character hex node ID
fn generate_node_id() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut id = String::with_capacity(40);
    for _ in 0..40 {
        id.push_str(&format!("{:x}", rng.gen_range(0..16)));
    }
    id
}

/// CRC16 implementation for Redis cluster key hashing
/// Uses the CCITT polynomial (0x1021) as per Redis implementation
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

/// Get slot ranges as a string (e.g., "0-5460 5461-10922")
fn get_slot_ranges(slots: &[bool; 16384]) -> String {
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < 16384 {
        if slots[i] {
            let start = i;
            while i < 16384 && slots[i] {
                i += 1;
            }
            let end = i - 1;
            if start == end {
                ranges.push(format!("{}", start));
            } else {
                ranges.push(format!("{}-{}", start, end));
            }
        } else {
            i += 1;
        }
    }
    ranges.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16() {
        // Test vector from Redis: "123456789" should hash to 0x31C3
        let hash = crc16(b"123456789");
        assert_eq!(hash, 0x31C3);
    }

    #[test]
    fn test_key_slot() {
        // Test hash tag extraction
        assert_eq!(
            ClusterState::key_slot(b"foo{bar}baz"),
            ClusterState::key_slot(b"bar")
        );
        assert_eq!(
            ClusterState::key_slot(b"foo{bar}"),
            ClusterState::key_slot(b"bar")
        );
        assert_eq!(
            ClusterState::key_slot(b"{bar}baz"),
            ClusterState::key_slot(b"bar")
        );
        assert_eq!(
            ClusterState::key_slot(b"foo{bar"),
            ClusterState::key_slot(b"foo{bar")
        ); // no closing brace
    }

    #[test]
    fn test_single_node_owns_all() {
        let cluster = ClusterState::new_single_node(6380);
        assert!(cluster.owns_key(b"test"));
        assert!(cluster.owns_key(b"any_key"));
        assert!(cluster.owns_slot(0));
        assert!(cluster.owns_slot(16383));
    }

    #[test]
    fn test_cluster_info() {
        let cluster = ClusterState::new_single_node(6380);
        let info = cluster.cluster_info();
        assert!(info.contains("cluster_state:ok"));
        assert!(info.contains("cluster_slots:16384"));
        assert!(info.contains("cluster_slots_assigned:16384"));
        assert!(info.contains("cluster_known_nodes:1"));
        assert!(info.contains("cluster_size:1"));
    }

    #[test]
    fn test_cluster_nodes() {
        let cluster = ClusterState::new_single_node(6380);
        let nodes = cluster.cluster_nodes();
        assert!(nodes.contains("myself,master"));
        assert!(nodes.contains("0-16383"));
    }

    #[test]
    fn test_cluster_myid() {
        let cluster = ClusterState::new_single_node(6380);
        assert_eq!(cluster.cluster_myid().len(), 40);
    }

    #[test]
    fn test_slot_ranges() {
        let mut slots = [false; 16384];
        slots[0] = true;
        slots[1] = true;
        slots[2] = true;
        slots[5] = true;
        slots[6] = true;
        let ranges = get_slot_ranges(&slots);
        assert!(ranges.contains("0-2"));
        assert!(ranges.contains("5-6"));
    }

    #[test]
    fn test_moved_error() {
        let cluster = ClusterState::new_single_node(6380);
        let error = cluster.moved_error(0);
        assert!(error.starts_with("MOVED 0"));
    }
}
