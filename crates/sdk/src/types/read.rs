//! Read consistency and write success types.

use inferadb_ledger_proto::proto;

/// Consistency level for read operations.
///
/// Controls whether reads are served from any replica (eventual) or must
/// go through the leader (linearizable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ReadConsistency {
    /// Reads from any replica (fastest, may be stale).
    #[default]
    Eventual,
    /// Reads from leader (strong consistency, higher latency).
    Linearizable,
}

impl ReadConsistency {
    /// Converts to protobuf enum value.
    pub(crate) fn to_proto(self) -> proto::ReadConsistency {
        match self {
            ReadConsistency::Eventual => proto::ReadConsistency::Eventual,
            ReadConsistency::Linearizable => proto::ReadConsistency::Linearizable,
        }
    }
}

/// Result of a successful write operation.
///
/// Contains the transaction ID, block height, and server-assigned sequence number
/// for the committed write.
///
/// A `WriteSuccess` value implies **WAL durability**: the Raft log has fsynced
/// the committed entry and every replica has applied it in-memory. State-DB
/// materialization is lazy and lands on the next checkpointer tick. Crash
/// recovery replays the WAL tail to rebuild any unpersisted state; replay is
/// idempotent and preserves batch atomicity. See
/// `docs/architecture/durability.md` for the full contract.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WriteSuccess {
    /// Unique transaction ID assigned by the server.
    pub tx_id: String,
    /// Block height where the transaction was committed.
    pub block_height: u64,
    /// Server-assigned sequence number for this write.
    ///
    /// The server assigns monotonically increasing sequence numbers at Raft commit
    /// time. This provides a total ordering of writes per (organization, vault, client)
    /// and can be used for audit trail continuity.
    pub assigned_sequence: u64,
}
