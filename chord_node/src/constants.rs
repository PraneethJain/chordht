pub const FINGER_TABLE_SIZE: usize = 64;
pub const REPLICATION_COUNT: usize = 2;
pub const SUCCESSOR_LIST_LIMIT: usize = 5;
pub const DEFAULT_PORT: u16 = 5000;
pub const LOCALHOST: &str = "127.0.0.1";

// Intervals
pub const STABILIZATION_INTERVAL_MS: u64 = 1000;
pub const FIX_FINGERS_INTERVAL_MS: u64 = 1000;
pub const CHECK_PREDECESSOR_INTERVAL_MS: u64 = 1000;
pub const MAINTAIN_REPLICATION_INTERVAL_MS: u64 = 1000;

// Delays
pub const LEAVE_EXIT_DELAY_MS: u64 = 100;
