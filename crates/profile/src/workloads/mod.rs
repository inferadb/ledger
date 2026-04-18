//! Workload presets. One module per preset; each exposes `run(&Harness, Duration)`.

pub mod check_heavy;
pub mod mixed_rw;
pub mod throughput_writes;
