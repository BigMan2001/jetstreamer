use crossbeam_channel::{Receiver, Sender, unbounded};
use dashmap::{DashMap, DashSet};
use futures_util::future::BoxFuture;
use reqwest::{Client, Url};
use solana_address::Address;
use solana_geyser_plugin_manager::{
    block_metadata_notifier_interface::BlockMetadataNotifier,
    geyser_plugin_service::GeyserPluginServiceError,
};
use solana_hash::Hash;
use solana_ledger::entry_notifier_interface::EntryNotifier;
use solana_reward_info::RewardInfo;
use solana_rpc::{
    optimistically_confirmed_bank_tracker::SlotNotification,
    transaction_notifier_interface::TransactionNotifier,
};
use solana_runtime::bank::{KeyedRewardsAndNumPartitions, RewardType};
use solana_sdk_ids::vote::id as vote_program_id;
use solana_transaction::versioned::VersionedTransaction;
use std::{
    fmt::Display,
    future::Future,
    io,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
};
use thiserror::Error;
use tokio::{
    sync::broadcast::{self, error::TryRecvError},
    time::timeout,
};
use std::path::Path;

use crate::{
    LOG_MODULE, SharedError,
    epochs::{epoch_to_slot_range, slot_to_epoch},
    epoch::{fetch_epoch_stream, read_proxies_file, HttpPool},
    index::{SLOT_OFFSET_INDEX, SlotOffsetIndexError},
    node_reader::NodeReader,
    utils,
};

// Timeout applied to each asynchronous firehose operation (fetching epoch stream, reading
// header, seeking, reading next block). Adjust here to tune stall detection/restart
// aggressiveness.
const OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
// Epochs earlier than this were bincode-encoded in Old Faithful.
const BINCODE_EPOCH_CUTOFF: u64 = 157;

fn poll_shutdown(
    flag: &Arc<std::sync::atomic::AtomicBool>,
    receiver: &mut Option<broadcast::Receiver<()>>,
) -> bool {
    if let Some(rx) = receiver {
        match rx.try_recv() {
            Ok(_) | Err(TryRecvError::Lagged(_)) => {
                flag.store(true, Ordering::SeqCst);
            }
            Err(TryRecvError::Closed) => {
                flag.store(true, Ordering::SeqCst);
            }
            Err(TryRecvError::Empty) => {}
        }
    }
    flag.load(Ordering::SeqCst)
}

fn is_shutdown_error(err: &FirehoseError) -> bool {
    fn is_interrupted(inner: &(dyn std::error::Error + 'static)) -> bool {
        inner
            .downcast_ref::<io::Error>()
            .map(|io_err| io_err.kind() == io::ErrorKind::Interrupted)
            .unwrap_or(false)
    }

    match err {
        FirehoseError::BlockHandlerError(inner)
        | FirehoseError::TransactionHandlerError(inner)
        | FirehoseError::EntryHandlerError(inner)
        | FirehoseError::RewardHandlerError(inner)
        | FirehoseError::OnStatsHandlerError(inner) => is_interrupted(inner.as_ref()),
        _ => false,
    }
}

/// Errors that can occur while streaming the firehose. Errors that can occur while streaming
/// the firehose.
#[derive(Debug, Error)]
pub enum FirehoseError {
    /// HTTP client error surfaced from `reqwest`.
    Reqwest(reqwest::Error),
    /// Failure while reading the Old Faithful CAR header.
    ReadHeader(SharedError),
    /// Error emitted by the Solana Geyser plugin service.
    GeyserPluginService(GeyserPluginServiceError),
    /// Transaction notifier could not be acquired from the Geyser service.
    FailedToGetTransactionNotifier,
    /// Failure while reading data until the next block boundary.
    ReadUntilBlockError(SharedError),
    /// Failure while fetching an individual block.
    GetBlockError(SharedError),
    /// Failed to decode a node at the given index.
    NodeDecodingError(usize, SharedError),
    /// Error surfaced when querying the slot offset index.
    SlotOffsetIndexError(SlotOffsetIndexError),
    /// Failure while seeking to a slot within the Old Faithful CAR stream.
    SeekToSlotError(SharedError),
    /// Error surfaced during the plugin `on_load` stage.
    OnLoadError(SharedError),
    /// Error emitted while invoking the stats handler.
    OnStatsHandlerError(SharedError),
    /// Timeout reached while waiting for a firehose operation.
    OperationTimeout(&'static str),
    /// Transaction handler returned an error.
    TransactionHandlerError(SharedError),
    /// Entry handler returned an error.
    EntryHandlerError(SharedError),
    /// Reward handler returned an error.
    RewardHandlerError(SharedError),
    /// Block handler returned an error.
    BlockHandlerError(SharedError),
}

unsafe impl Send for FirehoseError {}
unsafe impl Sync for FirehoseError {}

impl Display for FirehoseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FirehoseError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            FirehoseError::ReadHeader(error) => {
                write!(f, "Error reading header: {}", error)
            }
            FirehoseError::GeyserPluginService(geyser_plugin_service_error) => write!(
                f,
                "Error initializing geyser plugin service: {}",
                geyser_plugin_service_error
            ),
            FirehoseError::FailedToGetTransactionNotifier => write!(
                f,
                "Failed to get transaction notifier from GeyserPluginService"
            ),
            FirehoseError::ReadUntilBlockError(error) => {
                write!(f, "Error reading until block: {}", error)
            }
            FirehoseError::GetBlockError(error) => write!(f, "Error getting block: {}", error),
            FirehoseError::NodeDecodingError(item_index, error) => {
                write!(
                    f,
                    "Error seeking, reading data from, or decoding data for data node {}: {}",
                    item_index, error
                )
            }
            FirehoseError::SlotOffsetIndexError(slot_offset_index_error) => write!(
                f,
                "Error getting info from slot offset index: {}",
                slot_offset_index_error
            ),
            FirehoseError::SeekToSlotError(error) => {
                write!(f, "Error seeking to slot: {}", error)
            }
            FirehoseError::OnLoadError(error) => write!(f, "Error on load: {}", error),
            FirehoseError::OnStatsHandlerError(error) => {
                write!(f, "Stats handler error: {}", error)
            }
            FirehoseError::OperationTimeout(op) => {
                write!(f, "Timeout while waiting for operation: {}", op)
            }
            FirehoseError::TransactionHandlerError(error) => {
                write!(f, "Transaction handler error: {}", error)
            }
            FirehoseError::EntryHandlerError(error) => {
                write!(f, "Entry handler error: {}", error)
            }
            FirehoseError::RewardHandlerError(error) => {
                write!(f, "Reward handler error: {}", error)
            }
            FirehoseError::BlockHandlerError(error) => {
                write!(f, "Block handler error: {}", error)
            }
        }
    }
}

impl From<reqwest::Error> for FirehoseError {
    fn from(e: reqwest::Error) -> Self {
        FirehoseError::Reqwest(e)
    }
}

impl From<GeyserPluginServiceError> for FirehoseError {
    fn from(e: GeyserPluginServiceError) -> Self {
        FirehoseError::GeyserPluginService(e)
    }
}

impl From<SlotOffsetIndexError> for FirehoseError {
    fn from(e: SlotOffsetIndexError) -> Self {
        FirehoseError::SlotOffsetIndexError(e)
    }
}

/// Per-thread progress information emitted by the firehose runner.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ThreadStats {
    /// Identifier of the worker thread reporting the stats.
    pub thread_id: usize,
    /// Timestamp captured when the thread began processing.
    pub start_time: std::time::Instant,
    /// Timestamp captured when the thread finished, if finished.
    pub finish_time: Option<std::time::Instant>,
    /// Slot range currently assigned to the thread (half-open, may shrink on restart).
    pub slot_range: Range<u64>,
    /// Original slot range assigned to the thread (half-open, never modified).
    pub initial_slot_range: Range<u64>,
    /// Latest slot processed by the thread.
    pub current_slot: u64,
    /// Total slots processed by the thread.
    pub slots_processed: u64,
    /// Number of blocks successfully processed.
    pub blocks_processed: u64,
    /// Number of slots skipped by the cluster leader.
    pub leader_skipped_slots: u64,
    /// Total transactions processed.
    pub transactions_processed: u64,
    /// Total entries processed.
    pub entries_processed: u64,
    /// Total rewards processed.
    pub rewards_processed: u64,
}

/// Aggregated firehose statistics covering all worker threads.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Stats {
    /// Per-thread statistics for the current update.
    pub thread_stats: ThreadStats,
    /// Timestamp captured when processing began.
    pub start_time: std::time::Instant,
    /// Timestamp captured when all processing finished, if finished.
    pub finish_time: Option<std::time::Instant>,
    /// Slot range currently being processed (half-open [start, end)).
    pub slot_range: Range<u64>,
    /// Aggregate slots processed across all threads.
    pub slots_processed: u64,
    /// Aggregate blocks processed across all threads.
    pub blocks_processed: u64,
    /// Aggregate skipped slots across all threads.
    pub leader_skipped_slots: u64,
    /// Aggregate transactions processed across all threads.
    pub transactions_processed: u64,
    /// Aggregate entries processed across all threads.
    pub entries_processed: u64,
    /// Aggregate rewards processed across all threads.
    pub rewards_processed: u64,
    /// Transactions processed since the previous stats pulse.
    pub transactions_since_last_pulse: u64,
    /// Blocks processed since the previous stats pulse.
    pub blocks_since_last_pulse: u64,
    /// Slots processed since the previous stats pulse.
    pub slots_since_last_pulse: u64,
    /// Elapsed time since the previous stats pulse.
    pub time_since_last_pulse: std::time::Duration,
}

/// Configuration for periodic stats emission via a [`Handler`] callback.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StatsTracking<OnStats: Handler<Stats>> {
    /// Callback invoked whenever new stats are available.
    pub on_stats: OnStats,
    /// Emits a stats callback when the current slot is a multiple of this interval.
    pub tracking_interval_slots: u64,
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
async fn maybe_emit_stats<OnStats: Handler<Stats>>(
    stats_tracking: Option<&StatsTracking<OnStats>>,
    thread_index: usize,
    thread_stats: &ThreadStats,
    overall_slots_processed: &AtomicU64,
    overall_blocks_processed: &AtomicU64,
    overall_transactions_processed: &AtomicU64,
    overall_entries_processed: &AtomicU64,
    transactions_since_stats: &AtomicU64,
    blocks_since_stats: &AtomicU64,
    slots_since_stats: &AtomicU64,
    last_pulse: &Arc<AtomicU64>,
    base_instant: std::time::Instant,
) -> Result<(), (FirehoseError, u64)> {
    if let Some(stats_tracker) = stats_tracking {
        let total_slots = overall_slots_processed.load(Ordering::Relaxed);
        let total_blocks = overall_blocks_processed.load(Ordering::Relaxed);
        let total_transactions = overall_transactions_processed.load(Ordering::Relaxed);
        let total_entries = overall_entries_processed.load(Ordering::Relaxed);
        let now_nanos = base_instant.elapsed().as_nanos() as u64;
        let previous = last_pulse.swap(now_nanos, Ordering::Relaxed);
        let delta_nanos = now_nanos.saturating_sub(previous);
        let time_since_last_pulse = std::time::Duration::from_nanos(delta_nanos.max(1));
        let processed_transactions = transactions_since_stats.swap(0, Ordering::Relaxed);
        let processed_blocks = blocks_since_stats.swap(0, Ordering::Relaxed);
        let processed_slots = slots_since_stats.swap(0, Ordering::Relaxed);

        let stats = Stats {
            thread_stats: thread_stats.clone(),
            start_time: thread_stats.start_time,
            finish_time: thread_stats.finish_time,
            slot_range: thread_stats.slot_range.clone(),
            slots_processed: total_slots,
            blocks_processed: total_blocks,
            leader_skipped_slots: total_slots.saturating_sub(total_blocks),
            transactions_processed: total_transactions,
            entries_processed: total_entries,
            rewards_processed: thread_stats.rewards_processed,
            transactions_since_last_pulse: processed_transactions,
            blocks_since_last_pulse: processed_blocks,
            slots_since_last_pulse: processed_slots,
            time_since_last_pulse,
        };

        if let Err(e) = (stats_tracker.on_stats)(thread_index, stats).await {
            last_pulse.store(previous, Ordering::Relaxed);
            transactions_since_stats.fetch_add(processed_transactions, Ordering::Relaxed);
            blocks_since_stats.fetch_add(processed_blocks, Ordering::Relaxed);
            slots_since_stats.fetch_add(processed_slots, Ordering::Relaxed);
            return Err((
                FirehoseError::OnStatsHandlerError(e),
                thread_stats.current_slot,
            ));
        }
    }
    Ok(())
}

#[inline(always)]
fn fetch_add_if(tracking_enabled: bool, atomic: &AtomicU64, value: u64) {
    if tracking_enabled {
        atomic.fetch_add(value, Ordering::Relaxed);
    }
}

fn clear_pending_skip(map: &DashMap<usize, DashSet<u64>>, thread_id: usize, slot: u64) -> bool {
    map.get(&thread_id)
        .map(|set| set.remove(&slot).is_some())
        .unwrap_or(false)
}

fn decode_transaction_status_meta_from_frame(
    slot: u64,
    reassembled_metadata: Vec<u8>,
) -> Result<solana_transaction_status::TransactionStatusMeta, SharedError> {
    if reassembled_metadata.is_empty() {
        // Early epochs often omit metadata entirely.
        return Ok(solana_transaction_status::TransactionStatusMeta::default());
    }

    match utils::decompress_zstd(reassembled_metadata.clone()) {
        Ok(decompressed) => {
            decode_transaction_status_meta(slot, decompressed.as_slice()).map_err(|err| {
                Box::new(std::io::Error::other(format!(
                    "decode transaction metadata (slot {slot}): {err}"
                ))) as SharedError
            })
        }
        Err(decomp_err) => {
            // If the frame was not zstd-compressed (common for very early data), try to
            // decode the raw bytes directly before bailing.
            decode_transaction_status_meta(slot, reassembled_metadata.as_slice()).map_err(|err| {
                Box::new(std::io::Error::other(format!(
                    "transaction metadata not zstd-compressed for slot {slot}; raw decode failed (raw_err={err}, decompress_err={decomp_err})"
                ))) as SharedError
            })
        }
    }
}

fn decode_transaction_status_meta(
    slot: u64,
    metadata_bytes: &[u8],
) -> Result<solana_transaction_status::TransactionStatusMeta, SharedError> {
    let epoch = slot_to_epoch(slot);
    let mut bincode_err: Option<String> = None;
    if epoch < BINCODE_EPOCH_CUTOFF {
        match bincode::deserialize::<solana_storage_proto::StoredTransactionStatusMeta>(
            metadata_bytes,
        ) {
            Ok(stored) => return Ok(stored.into()),
            Err(err) => {
                bincode_err = Some(err.to_string());
            }
        }
    }

    let bin_err_for_proto = bincode_err.clone();
    let proto: solana_storage_proto::convert::generated::TransactionStatusMeta =
        prost_011::Message::decode(metadata_bytes).map_err(|err| {
            // If we already tried bincode, surface both failures for easier debugging.
            if let Some(ref bin_err) = bin_err_for_proto {
                Box::new(std::io::Error::other(format!(
                    "protobuf decode transaction metadata failed (epoch {epoch}); bincode failed earlier: {bin_err}; protobuf error: {err}"
                ))) as SharedError
            } else {
                Box::new(std::io::Error::other(format!(
                    "protobuf decode transaction metadata: {err}"
                ))) as SharedError
            }
        })?;

    proto.try_into().map_err(|err| {
        if let Some(ref bin_err) = bincode_err {
            Box::new(std::io::Error::other(format!(
                "convert transaction metadata proto failed (epoch {epoch}); bincode failed earlier: {bin_err}; conversion error: {err}"
            ))) as SharedError
        } else {
            Box::new(std::io::Error::other(format!(
                "convert transaction metadata proto: {err}"
            ))) as SharedError
        }
    })
}

#[cfg(test)]
mod metadata_decode_tests {
    use super::{decode_transaction_status_meta, decode_transaction_status_meta_from_frame};
    use solana_message::v0::LoadedAddresses;
    use solana_storage_proto::StoredTransactionStatusMeta;
    use solana_transaction_status::TransactionStatusMeta;

    fn sample_meta() -> TransactionStatusMeta {
        let mut meta = TransactionStatusMeta::default();
        meta.fee = 42;
        meta.pre_balances = vec![1, 2];
        meta.post_balances = vec![3, 4];
        meta.log_messages = Some(vec!["hello".into()]);
        meta.pre_token_balances = Some(Vec::new());
        meta.post_token_balances = Some(Vec::new());
        meta.rewards = Some(Vec::new());
        meta.compute_units_consumed = Some(7);
        meta.cost_units = Some(9);
        meta.loaded_addresses = LoadedAddresses::default();
        meta
    }

    #[test]
    fn decodes_bincode_metadata_for_early_epochs() {
        let stored = StoredTransactionStatusMeta {
            status: Ok(()),
            fee: 42,
            pre_balances: vec![1, 2],
            post_balances: vec![3, 4],
            inner_instructions: None,
            log_messages: Some(vec!["hello".into()]),
            pre_token_balances: Some(Vec::new()),
            post_token_balances: Some(Vec::new()),
            rewards: Some(Vec::new()),
            return_data: None,
            compute_units_consumed: Some(7),
            cost_units: Some(9),
        };
        let bytes = bincode::serialize(&stored).expect("bincode serialize");
        let decoded = decode_transaction_status_meta(0, &bytes).expect("decode");
        assert_eq!(decoded, TransactionStatusMeta::from(stored));
    }

    #[test]
    fn decodes_protobuf_metadata_for_later_epochs() {
        let meta = sample_meta();
        let generated: solana_storage_proto::convert::generated::TransactionStatusMeta =
            meta.clone().into();
        let bytes = prost_011::Message::encode_to_vec(&generated);
        let decoded = decode_transaction_status_meta(157 * 432000, &bytes).expect("decode");
        assert_eq!(decoded, meta);
    }

    #[test]
    fn falls_back_to_proto_when_early_epoch_bytes_are_proto() {
        let meta = sample_meta();
        let generated: solana_storage_proto::convert::generated::TransactionStatusMeta =
            meta.clone().into();
        let bytes = prost_011::Message::encode_to_vec(&generated);
        // Epoch 100 should try bincode first; if those bytes are proto, we must fall back.
        let decoded = decode_transaction_status_meta(100 * 432000, &bytes).expect("decode");
        assert_eq!(decoded, meta);
    }

    #[test]
    fn empty_frame_decodes_to_default() {
        let decoded = decode_transaction_status_meta_from_frame(0, Vec::new()).expect("decode");
        assert_eq!(decoded, TransactionStatusMeta::default());
    }

    #[test]
    fn raw_bincode_frame_without_zstd_still_decodes() {
        let stored = StoredTransactionStatusMeta {
            status: Ok(()),
            fee: 1,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: Some(Vec::new()),
            post_token_balances: Some(Vec::new()),
            rewards: Some(Vec::new()),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        };
        let raw_bytes = bincode::serialize(&stored).expect("serialize");
        let decoded =
            decode_transaction_status_meta_from_frame(0, raw_bytes).expect("decode fallback");
        assert_eq!(decoded, TransactionStatusMeta::from(stored));
    }
}

/// Firehose transaction payload passed to [`Handler`] callbacks.
#[derive(Debug, Clone)]
pub struct TransactionData {
    /// Slot that contains the transaction.
    pub slot: u64,
    /// Index of the transaction within the slot.
    pub transaction_slot_index: usize,
    /// Transaction signature.
    pub signature: solana_signature::Signature,
    /// Hash of the transaction message.
    pub message_hash: Hash,
    /// Indicates whether the transaction is a vote.
    pub is_vote: bool,
    /// Status metadata returned by the Solana runtime.
    pub transaction_status_meta: solana_transaction_status::TransactionStatusMeta,
    /// Fully decoded transaction.
    pub transaction: VersionedTransaction,
    /// Block time.
    pub blocktime: i64
}

/// Block entry metadata passed to [`Handler`] callbacks.
#[derive(Debug, Clone)]
pub struct EntryData {
    /// Slot that generated the entry.
    pub slot: u64,
    /// Index of the entry within the slot.
    pub entry_index: usize,
    /// Range of transaction indexes covered by the entry.
    pub transaction_indexes: Range<usize>,
    /// Number of hashes associated with the entry.
    pub num_hashes: u64,
    /// Entry hash.
    pub hash: Hash,
}

/// Reward data conveyed to reward [`Handler`] callbacks.
#[derive(Debug, Clone)]
pub struct RewardsData {
    /// Slot the rewards correspond to.
    pub slot: u64,
    /// Reward recipients and their associated reward information.
    pub rewards: Vec<(Address, RewardInfo)>,
}

/// Block-level data streamed to block handlers.
#[derive(Debug)]
pub enum BlockData {
    /// Fully populated block payload with ledger metadata.
    Block {
        /// Parent slot number.
        parent_slot: u64,
        /// Parent block hash.
        parent_blockhash: Hash,
        /// Current block slot.
        slot: u64,
        /// Current block hash.
        blockhash: Hash,
        /// Rewards keyed by account and partition information.
        rewards: KeyedRewardsAndNumPartitions,
        /// Optional Unix timestamp for the block.
        block_time: Option<i64>,
        /// Optional ledger block height.
        block_height: Option<u64>,
        /// Number of executed transactions in the block.
        executed_transaction_count: u64,
        /// Number of entries contained in the block.
        entry_count: u64,
    },
    /// Marker indicating the slot appears skipped (either truly skipped or it is late and will
    /// arrive out of order).
    PossibleLeaderSkipped {
        /// Slot number that either lacked a block or may still arrive later.
        slot: u64,
    },
}

impl BlockData {
    /// Returns the slot associated with this block or skipped slot.
    #[inline(always)]
    pub const fn slot(&self) -> u64 {
        match self {
            BlockData::Block { slot, .. } => *slot,
            BlockData::PossibleLeaderSkipped { slot } => *slot,
        }
    }

    /// Returns `true` if this record currently represents a missing/possibly skipped slot.
    #[inline(always)]
    pub const fn was_skipped(&self) -> bool {
        matches!(self, BlockData::PossibleLeaderSkipped { .. })
    }

    /// Returns the optional block time when available.
    #[inline(always)]
    pub const fn block_time(&self) -> Option<i64> {
        match self {
            BlockData::Block { block_time, .. } => *block_time,
            BlockData::PossibleLeaderSkipped { .. } => None,
        }
    }
}

type HandlerResult = Result<(), SharedError>;
type HandlerFuture = BoxFuture<'static, HandlerResult>;

/// Asynchronous callback invoked for each firehose event of type `Data`.
pub trait Handler<Data>: Fn(usize, Data) -> HandlerFuture + Send + Sync + Clone + 'static {}

impl<Data, F> Handler<Data> for F where
    F: Fn(usize, Data) -> HandlerFuture + Send + Sync + Clone + 'static
{
}

/// Function pointer alias for [`Handler`] callbacks.
pub type HandlerFn<Data> = fn(usize, Data) -> HandlerFuture;
/// Convenience alias for block handlers accepted by [`firehose`].
pub type OnBlockFn = HandlerFn<BlockData>;
/// Convenience alias for transaction handlers accepted by [`firehose`].
pub type OnTxFn = HandlerFn<TransactionData>;
/// Convenience alias for entry handlers accepted by [`firehose`].
pub type OnEntryFn = HandlerFn<EntryData>;
/// Convenience alias for reward handlers accepted by [`firehose`].
pub type OnRewardFn = HandlerFn<RewardsData>;
/// Type alias for [`StatsTracking`] using simple function pointers.
pub type StatsTracker = StatsTracking<HandlerFn<Stats>>;
/// Convenience alias for firehose error handlers.
pub type OnErrorFn = HandlerFn<FirehoseErrorContext>;
/// Convenience alias for stats tracking handlers accepted by [`firehose`].
pub type OnStatsTrackingFn = StatsTracking<HandlerFn<Stats>>;

/// Metadata describing a firehose worker failure.
#[derive(Clone, Debug)]
pub struct FirehoseErrorContext {
    /// Thread index that encountered the error.
    pub thread_id: usize,
    /// Slot the worker was processing when the error surfaced.
    pub slot: u64,
    /// Epoch derived from the failing slot.
    pub epoch: u64,
    /// Stringified error payload for display/logging.
    pub error_message: String,
}

/// Streams transactions for selected slots using the Firehose protocol.
///
/// This is a pruned and optimized variant of the original firehose:
/// - Only the `on_tx` handler is invoked
/// - All other handlers are accepted for API compatibility but are ignored
/// - Transactions are decoded **only** for slots present in `slots_filter`
/// - Block time is propagated to `TransactionData.blocktime` from block metadata
///
/// The requested `slot_range` is half-open: `[start, end)`.
/// On recoverable errors, the runner restarts from the last processed slot
/// to maintain full coverage.
///
/// # Parameters
/// - `threads`: Number of parallel firehose workers
/// - `slot_range`: Slot range to scan (half-open)
/// - `slots_filter`: Exact set of slots to process transactions for
/// - `on_tx`: Transaction handler (required)
/// - `shutdown_signal`: Optional shutdown broadcast receiver
///
/// # Returns
/// Returns `Ok(())` on successful completion, or
/// `(FirehoseError, slot)` on failure.
#[inline]
#[allow(clippy::too_many_arguments)]
pub async fn firehose<OnBlock, OnTransaction, OnEntry, OnRewards, OnStats, OnError>(
    threads: u64,
    slot_range: Range<u64>,
    slots_filter: Arc<std::collections::HashSet<u64>>, // ✅ ONLY NEW PARAM
    proxies_file: Option<impl AsRef<Path>>, // ✅ NEW PARAM
    _on_block: Option<OnBlock>,
    on_tx: Option<OnTransaction>,
    _on_entry: Option<OnEntry>,
    _on_rewards: Option<OnRewards>,
    _on_error: Option<OnError>,
    _stats_tracking: Option<StatsTracking<OnStats>>,
    shutdown_signal: Option<broadcast::Receiver<()>>,
) -> Result<(), (FirehoseError, u64)>
where
    OnBlock: Handler<BlockData>,
    OnTransaction: Handler<TransactionData>,
    OnEntry: Handler<EntryData>,
    OnRewards: Handler<RewardsData>,
    OnStats: Handler<Stats>,
    OnError: Handler<FirehoseErrorContext>,
{
    if threads == 0 {
        return Err((
            FirehoseError::OnLoadError("threads must be > 0".into()),
            slot_range.start,
        ));
    }

    let on_tx = on_tx.ok_or((
        FirehoseError::OnLoadError("on_tx handler required".into()),
        slot_range.start,
    ))?;

    let client = Client::new();


    // ✅ Build proxy pool (or direct) for Old Faithful epoch CAR fetching
    let pool = match proxies_file {
        None => Arc::new(HttpPool::new(Vec::new())),
        Some(p) => {
            let proxies = read_proxies_file(p.as_ref()).map_err(|e| {
                (
                    FirehoseError::OnLoadError(Box::new(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "failed to read proxies file {}: {}",
                            p.as_ref().display(),
                            e
                        ),
                    ))),
                    slot_range.start,
                )
            })?;
            Arc::new(HttpPool::new(proxies))
        }
    };

    let slot_range = Arc::new(slot_range);
    let subranges = generate_subranges(&slot_range, threads);

    let shutdown_flag = Arc::new(AtomicBool::new(false));
    if let Some(rx) = shutdown_signal.as_ref() {
        let mut rx = rx.resubscribe();
        let flag = shutdown_flag.clone();
        tokio::spawn(async move {
            let _ = rx.recv().await;
            flag.store(true, Ordering::SeqCst);
        });
    }

    let mut handles: Vec<tokio::task::JoinHandle<Result<(), (FirehoseError, u64)>>> =
        Vec::with_capacity(subranges.len());

    for (thread_id, mut range) in subranges.into_iter().enumerate() {
        let pool = pool.clone(); // <-- clone for this task
        let client = client.clone();
        let slots_filter = slots_filter.clone();
        let on_tx = on_tx.clone();
        let shutdown_flag = shutdown_flag.clone();
        let mut shutdown_rx = shutdown_signal.as_ref().map(|r| r.resubscribe());

        let handle = tokio::spawn(async move {
            let mut last_processed_slot = range.start.saturating_sub(1);

            while let Err((err, restart_slot)) = async {
                if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                    return Ok(());
                }

                let epoch_range =
                    slot_to_epoch(range.start)..=slot_to_epoch(range.end - 1);

                for epoch in epoch_range {
                    if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                        return Ok(());
                    }

                    let stream = fetch_epoch_stream(epoch, pool.clone()).await;


                    let mut reader = NodeReader::new(stream, pool.clone());
                    reader
                        .read_raw_header()
                        .await
                        .map_err(|e| (FirehoseError::ReadHeader(e), range.start))?;

                    let (epoch_start, epoch_end) = epoch_to_slot_range(epoch);
                    let local_start = range.start.max(epoch_start);
                    let local_end = (range.end - 1).min(epoch_end);

                    if local_start > local_end {
                        continue;
                    }

                    if local_start > epoch_start {
                        reader
                            .seek_to_slot(local_start - 1)
                            .await
                            .map_err(|e| (e, local_start))?;
                    }

                    loop {
                        if poll_shutdown(&shutdown_flag, &mut shutdown_rx) {
                            return Ok(());
                        }

                        let nodes = reader
                            .read_until_block()
                            .await
                            .map_err(|e| {
                                (
                                    FirehoseError::ReadUntilBlockError(e),
                                    last_processed_slot,
                                )
                            })?;

                        if nodes.is_empty() {
                            break;
                        }

                        let block = nodes
                            .get_block()
                            .map_err(|e| (FirehoseError::GetBlockError(e), last_processed_slot))?;

                        let slot = block.slot;
                        if slot < range.start || slot >= range.end {
                            continue;
                        }

                        last_processed_slot = slot;

                        let blocktime = block.meta.blocktime as i64;

                        for node in &nodes.0 {
                            if let crate::node::Node::Transaction(tx) = node.get_node() {
                                let versioned_tx = tx.as_parsed().map_err(|e| {
                                    (
                                        FirehoseError::NodeDecodingError(0, e),
                                        slot,
                                    )
                                })?;

                                let frames = nodes
                                    .reassemble_dataframes(tx.metadata.clone())
                                    .map_err(|e| {
                                        (
                                            FirehoseError::NodeDecodingError(0, e),
                                            slot,
                                        )
                                    })?;

                                let status_meta =
                                    decode_transaction_status_meta_from_frame(slot, frames)
                                        .map_err(|e| {
                                            (
                                                FirehoseError::NodeDecodingError(0, e),
                                                slot,
                                            )
                                        })?;

                                let message_hash = versioned_tx.message.hash();
                                let signature = *versioned_tx
                                    .signatures
                                    .first()
                                    .ok_or((
                                        FirehoseError::OnLoadError(
                                            "tx missing signature".into(),
                                        ),
                                        slot,
                                    ))?;

                                let is_vote = is_simple_vote_transaction(&versioned_tx);

                                on_tx(
                                    thread_id,
                                    TransactionData {
                                        slot,
                                        transaction_slot_index: tx.index.unwrap() as usize,
                                        signature,
                                        message_hash,
                                        is_vote,
                                        transaction_status_meta: status_meta,
                                        transaction: versioned_tx,
                                        blocktime,
                                    },
                                )
                                .await
                                .map_err(|e| {
                                    (
                                        FirehoseError::TransactionHandlerError(e),
                                        slot,
                                    )
                                })?;
                            }
                        }
                    }
                }

                Ok(())
            }
            .await
            {
                if is_shutdown_error(&err) {
                    return Ok(());
                }

                if matches!(err, FirehoseError::SlotOffsetIndexError(_)) {
                    SLOT_OFFSET_INDEX.invalidate_epoch(slot_to_epoch(restart_slot));
                }

                range.start = restart_slot.max(last_processed_slot + 1);
            }

            Ok(())
        });

        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    Ok(())
}

#[inline]
fn is_simple_vote_transaction(versioned_tx: &VersionedTransaction) -> bool {
    if !(1..=2).contains(&versioned_tx.signatures.len()) {
        return false;
    }

    if !matches!(
        versioned_tx.version(),
        solana_transaction::versioned::TransactionVersion::Legacy(_)
    ) {
        return false;
    }

    let instructions = versioned_tx.message.instructions();
    if instructions.len() != 1 {
        return false;
    }

    let program_index = instructions[0].program_id_index as usize;
    versioned_tx
        .message
        .static_account_keys()
        .get(program_index)
        .map(|program_id| program_id == &vote_program_id())
        .unwrap_or(false)
}

#[inline(always)]
fn convert_proto_rewards(
    proto_rewards: &solana_storage_proto::convert::generated::Rewards,
) -> Result<Vec<(Address, RewardInfo)>, SharedError> {
    let mut keyed_rewards = Vec::with_capacity(proto_rewards.rewards.len());
    for proto_reward in proto_rewards.rewards.iter() {
        let reward = RewardInfo {
            reward_type: match proto_reward.reward_type - 1 {
                0 => RewardType::Fee,
                1 => RewardType::Rent,
                2 => RewardType::Staking,
                3 => RewardType::Voting,
                typ => {
                    return Err(Box::new(std::io::Error::other(format!(
                        "unsupported reward type {}",
                        typ
                    ))));
                }
            },
            lamports: proto_reward.lamports,
            post_balance: proto_reward.post_balance,
            commission: proto_reward.commission.parse::<u8>().ok(),
        };
        let pubkey = proto_reward
            .pubkey
            .parse::<Address>()
            .map_err(|err| Box::new(err) as SharedError)?;
        keyed_rewards.push((pubkey, reward));
    }
    Ok(keyed_rewards)
}

#[inline]
/// Splits `slot_range` into nearly-even sub-ranges for the given thread count.
pub fn generate_subranges(slot_range: &Range<u64>, threads: u64) -> Vec<Range<u64>> {
    let total = slot_range.end - slot_range.start;
    let slots_per_thread = total / threads;
    let remainder = total % threads;

    let ranges: Vec<Range<u64>> = (0..threads)
        .map(|i| {
            // Distribute remainder slots to the first `remainder` threads
            let extra_slot = if i < remainder { 1 } else { 0 };
            let start = slot_range.start + i * slots_per_thread + i.min(remainder);
            let end = start + slots_per_thread + extra_slot;
            start..end
        })
        .collect();

    // Verify that ranges cover all slots exactly
    let total_covered: u64 = ranges.iter().map(|r| r.end - r.start).sum();
    assert_eq!(
        total_covered, total,
        "Range generation failed: {} threads should cover {} slots but only cover {}",
        threads, total, total_covered
    );

    // Verify no gaps between ranges
    for i in 1..ranges.len() {
        assert_eq!(
            ranges[i - 1].end,
            ranges[i].start,
            "Gap found between thread {} (ends at {}) and thread {} (starts at {})",
            i - 1,
            ranges[i - 1].end,
            i,
            ranges[i].start
        );
    }

    log::info!(
        target: LOG_MODULE,
        "Generated {} thread ranges covering {} slots total",
        threads,
        total_covered
    );
    ranges
}

fn human_readable_duration(duration: std::time::Duration) -> String {
    if duration.is_zero() {
        return "0s".into();
    }
    let total_secs = duration.as_secs();
    if total_secs < 60 {
        let secs_f = duration.as_secs_f64();
        if total_secs == 0 {
            format!("{:.2}s", secs_f)
        } else if duration.subsec_millis() == 0 {
            format!("{}s", total_secs)
        } else {
            format!("{:.2}s", secs_f)
        }
    } else {
        let mut secs = total_secs;
        let days = secs / 86_400;
        secs %= 86_400;
        let hours = secs / 3_600;
        secs %= 3_600;
        let minutes = secs / 60;
        secs %= 60;
        if days > 0 {
            if hours > 0 {
                format!("{days}d{hours}h")
            } else {
                format!("{days}d")
            }
        } else if hours > 0 {
            if minutes > 0 {
                format!("{hours}h{minutes}m")
            } else {
                format!("{hours}h")
            }
        } else if minutes > 0 {
            if secs > 0 {
                format!("{minutes}m{secs}s")
            } else {
                format!("{minutes}m")
            }
        } else {
            format!("{secs}s")
        }
    }
}

#[cfg(test)]
fn log_stats_handler(thread_id: usize, stats: Stats) -> HandlerFuture {
    Box::pin(async move {
        let elapsed = stats.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let tps = if elapsed_secs > 0.0 {
            stats.transactions_processed as f64 / elapsed_secs
        } else {
            0.0
        };
        log::info!(
            target: LOG_MODULE,
            "thread {thread_id} stats: current_slot={}, slots_processed={}, blocks_processed={}, txs={}, entries={}, rewards={}, elapsed_s={:.2}, tps={:.2}",
            stats.thread_stats.current_slot,
            stats.slots_processed,
            stats.blocks_processed,
            stats.transactions_processed,
            stats.entries_processed,
            stats.rewards_processed,
            elapsed_secs,
            tps
        );
        Ok(())
    })
}

#[cfg(test)]
use futures_util::FutureExt;
#[cfg(test)]
use serial_test::serial;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_epoch_800() {
    use dashmap::DashSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    solana_logger::setup_with_default("info");
    const THREADS: usize = 4;
    const NUM_SLOTS_TO_COVER: u64 = 50;
    static PREV_BLOCK: [AtomicU64; THREADS] = [const { AtomicU64::new(0) }; THREADS];
    static NUM_SKIPPED_BLOCKS: AtomicU64 = AtomicU64::new(0);
    static NUM_BLOCKS: AtomicU64 = AtomicU64::new(0);
    static SEEN_SKIPPED: OnceLock<DashSet<u64>> = OnceLock::new();
    static SEEN_SLOTS: OnceLock<DashSet<u64>> = OnceLock::new();
    static MIN_TRANSACTIONS: AtomicU64 = AtomicU64::new(u64::MAX);
    let stats_tracking = StatsTracking {
        on_stats: log_stats_handler,
        tracking_interval_slots: 10,
    };

    for prev in PREV_BLOCK.iter() {
        prev.store(0, Ordering::Relaxed);
    }
    NUM_SKIPPED_BLOCKS.store(0, Ordering::Relaxed);
    NUM_BLOCKS.store(0, Ordering::Relaxed);
    MIN_TRANSACTIONS.store(u64::MAX, Ordering::Relaxed);
    SEEN_SLOTS.get_or_init(DashSet::new).clear();
    SEEN_SKIPPED.get_or_init(DashSet::new).clear();

    firehose(
        THREADS.try_into().unwrap(),
        (345600000 - NUM_SLOTS_TO_COVER / 2)..(345600000 + NUM_SLOTS_TO_COVER / 2),
        Some(|thread_id: usize, block: BlockData| {
            async move {
                let _prev =
                    PREV_BLOCK[thread_id % PREV_BLOCK.len()].swap(block.slot(), Ordering::Relaxed);
                if block.was_skipped() {
                    log::info!(
                        target: LOG_MODULE,
                        "leader skipped block {} on thread {}",
                        block.slot(),
                        thread_id,
                    );
                } else {
                    /*log::info!(
                        target: LOG_MODULE,
                        "got block {} on thread {}",
                        block.slot(),
                        thread_id,
                    );*/
                }

                let first_time = SEEN_SLOTS.get_or_init(DashSet::new).insert(block.slot());
                if block.was_skipped() {
                    NUM_SKIPPED_BLOCKS.fetch_add(1, Ordering::Relaxed);
                    SEEN_SKIPPED.get_or_init(DashSet::new).insert(block.slot());
                } else {
                    if first_time {
                        NUM_BLOCKS.fetch_add(1, Ordering::Relaxed);
                        if let BlockData::Block {
                            executed_transaction_count,
                            ..
                        } = &block
                        {
                            let executed = *executed_transaction_count;
                            let _ = MIN_TRANSACTIONS.fetch_update(
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                                |current| {
                                    if executed < current {
                                        Some(executed)
                                    } else {
                                        None
                                    }
                                },
                            );
                        }
                    }
                }
                Ok(())
            }
            .boxed()
        }),
        None::<OnTxFn>,
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        Some(stats_tracking),
        None,
    )
    .await
    .unwrap();
    let seen = SEEN_SLOTS.get_or_init(DashSet::new).len() as u64;
    assert_eq!(
        seen, NUM_SLOTS_TO_COVER,
        "expected to see exactly {NUM_SLOTS_TO_COVER} unique slots, saw {seen}"
    );
    let mut skipped: Vec<u64> = SEEN_SKIPPED
        .get_or_init(DashSet::new)
        .iter()
        .map(|v| *v)
        .collect();
    skipped.sort_unstable();
    // 345600000 is present but empty; still emitted as a block. Skip set should not include it.
    const EXPECTED_SKIPPED: [u64; 6] = [
        345_600_004,
        345_600_005,
        345_600_008,
        345_600_009,
        345_600_010,
        345_600_011,
    ];
    assert_eq!(skipped, EXPECTED_SKIPPED, "unexpected skipped slots");
    assert!(NUM_BLOCKS.load(Ordering::Relaxed) > 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_target_slot_transactions() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    solana_logger::setup_with_default("info");
    const TARGET_SLOT: u64 = 376_273_722;
    const SLOT_RADIUS: u64 = 50;
    const EXPECTED_TRANSACTIONS: u64 = 1414;
    const EXPECTED_NON_VOTE_TRANSACTIONS: u64 = 511;
    static FOUND: AtomicBool = AtomicBool::new(false);
    static OBSERVED_TXS: AtomicU64 = AtomicU64::new(0);
    static OBSERVED_NON_VOTE: AtomicU64 = AtomicU64::new(0);

    FOUND.store(false, Ordering::Relaxed);
    OBSERVED_TXS.store(0, Ordering::Relaxed);
    OBSERVED_NON_VOTE.store(0, Ordering::Relaxed);

    firehose(
        4,
        (TARGET_SLOT - SLOT_RADIUS)..(TARGET_SLOT + SLOT_RADIUS),
        Some(|_thread_id: usize, block: BlockData| {
            async move {
                if block.slot() == TARGET_SLOT {
                    assert!(
                        !block.was_skipped(),
                        "target slot {TARGET_SLOT} was marked leader skipped",
                    );
                    if let BlockData::Block {
                        executed_transaction_count,
                        ..
                    } = block
                    {
                        OBSERVED_TXS.store(executed_transaction_count, Ordering::Relaxed);
                        FOUND.store(true, Ordering::Relaxed);
                        assert_eq!(
                            executed_transaction_count, EXPECTED_TRANSACTIONS,
                            "unexpected transaction count for slot {TARGET_SLOT}"
                        );
                        assert_eq!(
                            OBSERVED_NON_VOTE.load(Ordering::Relaxed),
                            EXPECTED_NON_VOTE_TRANSACTIONS,
                            "unexpected non-vote transaction count for slot {TARGET_SLOT}"
                        );
                    }
                }
                Ok(())
            }
            .boxed()
        }),
        Some(|_thread_id: usize, transaction: TransactionData| {
            async move {
                if transaction.slot == TARGET_SLOT && !transaction.is_vote {
                    OBSERVED_NON_VOTE.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            .boxed()
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap();

    assert!(
        FOUND.load(Ordering::Relaxed),
        "target slot was not processed"
    );
    assert_eq!(
        OBSERVED_TXS.load(Ordering::Relaxed),
        EXPECTED_TRANSACTIONS,
        "recorded transaction count mismatch"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_epoch_850_votes_present() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    solana_logger::setup_with_default("info");
    const TARGET_SLOT: u64 = 367_200_100; // epoch 850
    const SLOT_RADIUS: u64 = 10;
    static SEEN_BLOCK: AtomicBool = AtomicBool::new(false);
    static VOTE_TXS: AtomicU64 = AtomicU64::new(0);
    static TOTAL_TXS: AtomicU64 = AtomicU64::new(0);

    SEEN_BLOCK.store(false, Ordering::Relaxed);
    VOTE_TXS.store(0, Ordering::Relaxed);
    TOTAL_TXS.store(0, Ordering::Relaxed);

    firehose(
        2,
        (TARGET_SLOT - SLOT_RADIUS)..(TARGET_SLOT + SLOT_RADIUS),
        Some(|_thread_id: usize, block: BlockData| {
            async move {
                if block.slot() == TARGET_SLOT {
                    assert!(
                        !block.was_skipped(),
                        "target slot {TARGET_SLOT} was marked leader skipped",
                    );
                    SEEN_BLOCK.store(true, Ordering::Relaxed);
                }
                Ok(())
            }
            .boxed()
        }),
        Some(|_thread_id: usize, transaction: TransactionData| {
            async move {
                if transaction.slot == TARGET_SLOT {
                    TOTAL_TXS.fetch_add(1, Ordering::Relaxed);
                    if transaction.is_vote {
                        VOTE_TXS.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(())
            }
            .boxed()
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap();

    assert!(
        SEEN_BLOCK.load(Ordering::Relaxed),
        "target slot was not processed"
    );
    assert!(
        TOTAL_TXS.load(Ordering::Relaxed) > 0,
        "no transactions counted in target slot"
    );
    assert_eq!(VOTE_TXS.load(Ordering::Relaxed), 991);
}

#[cfg(test)]
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_restart_loses_coverage_without_reset() {
    use std::collections::HashMap;
    solana_logger::setup_with_default("info");
    const THREADS: usize = 1;
    const START_SLOT: u64 = 345_600_000;
    const NUM_SLOTS: u64 = 8;

    static COVERAGE: OnceLock<Mutex<HashMap<u64, u32>>> = OnceLock::new();
    COVERAGE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .clear();
    static FAIL_TRIGGERED: AtomicBool = AtomicBool::new(false);
    static SEEN_BLOCKS: AtomicU64 = AtomicU64::new(0);
    FAIL_TRIGGERED.store(false, Ordering::Relaxed);
    SEEN_BLOCKS.store(0, Ordering::Relaxed);

    firehose(
        THREADS.try_into().unwrap(),
        START_SLOT..(START_SLOT + NUM_SLOTS),
        Some(|_thread_id: usize, block: BlockData| {
            async move {
                // Force an error after at least one block has been seen so restart happens mid-range.
                if !block.was_skipped()
                    && SEEN_BLOCKS.load(Ordering::Relaxed) > 0
                    && !FAIL_TRIGGERED.swap(true, Ordering::SeqCst)
                {
                    return Err("synthetic handler failure to exercise restart".into());
                }
                let mut coverage = COVERAGE
                    .get_or_init(|| Mutex::new(HashMap::new()))
                    .lock()
                    .unwrap();
                *coverage.entry(block.slot()).or_insert(0) += 1;
                if !block.was_skipped() {
                    SEEN_BLOCKS.fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            .boxed()
        }),
        None::<OnTxFn>,
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap();

    let coverage = COVERAGE.get().unwrap().lock().unwrap();
    for slot in START_SLOT..(START_SLOT + NUM_SLOTS) {
        assert!(
            coverage.contains_key(&slot),
            "missing coverage for slot {slot} after restart"
        );
    }
}

#[cfg(test)]
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_firehose_gap_coverage_near_known_missing_range() {
    use std::collections::HashSet;
    solana_logger::setup_with_default("info");
    const GAP_START: u64 = 378864000;
    const START_SLOT: u64 = GAP_START - 1000;
    const END_SLOT: u64 = GAP_START + 1000;
    const THREADS: usize = 16;

    static COVERAGE: OnceLock<Mutex<HashSet<u64>>> = OnceLock::new();
    COVERAGE
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .clear();

    firehose(
        THREADS.try_into().unwrap(),
        START_SLOT..(END_SLOT + 1),
        Some(|_thread_id: usize, block: BlockData| {
            async move {
                if block.was_skipped() {
                    return Ok(());
                }
                let slot = block.slot();
                COVERAGE
                    .get_or_init(|| Mutex::new(HashSet::new()))
                    .lock()
                    .unwrap()
                    .insert(slot);
                Ok(())
            }
            .boxed()
        }),
        None::<OnTxFn>,
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    )
    .await
    .unwrap();

    let mut coverage = COVERAGE
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap()
        .clone();

    // ignore a known 4-slot leader skipped gap
    coverage.insert(378864396);
    coverage.insert(378864397);
    coverage.insert(378864398);
    coverage.insert(378864399);

    let expected: Vec<u64> = (START_SLOT..=END_SLOT).collect();
    let missing: Vec<u64> = expected
        .iter()
        .copied()
        .filter(|slot| !coverage.contains(slot))
        .collect();
    assert!(
        missing.is_empty(),
        "missing slots in {START_SLOT}..={END_SLOT}; count={}, first few={:?}",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
}
