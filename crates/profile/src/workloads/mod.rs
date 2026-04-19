//! Workload presets. One module per preset; each exposes `run(&Harness, Duration)`.

pub mod check_heavy;
pub mod entity_reads;
pub mod mixed_rw;
pub mod relationship_reads;
pub mod relationship_writes;
pub mod throughput_writes;
