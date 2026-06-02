use {
    futures::future::join,
    serde::Deserialize,
    solana_client::{client_error, nonblocking::rpc_client::RpcClient},
    solana_clock::DEFAULT_SLOTS_PER_EPOCH,
    solana_pubkey::Pubkey,
    std::{
        str::FromStr,
        sync::{Arc, RwLock, atomic::AtomicBool},
    },
    tokio::task::JoinHandle,
};

pub const DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(300);

///
/// A compact representation of the leader schedule for an epoch,
/// where each leader pubkey appears once for every 4 consecutive slots they lead.
///
#[derive(Clone, Debug)]
pub struct CompactSortedSchedule {
    pub first_slot: u64,
    schedule: Vec<Pubkey>,
}

impl CompactSortedSchedule {
    #[inline]
    pub fn last_slot(&self) -> u64 {
        self.first_slot + DEFAULT_SLOTS_PER_EPOCH - 1
    }

    ///
    /// Get the leader pubkey for a given slot in the epoch
    ///
    pub fn get(&self, slot: &u64) -> Option<&Pubkey> {
        let first_slot = self.first_slot;
        let max_slot = first_slot + DEFAULT_SLOTS_PER_EPOCH;
        if slot < &first_slot || slot >= &max_slot {
            return None;
        }
        let slot_idx = slot.saturating_sub(first_slot);
        let slot_idx = slot_idx / 4;
        // get the nearest leader boundary (every 4 slots)
        self.schedule.get(slot_idx as usize)
    }

    ///
    /// Iterator over (slot, leader_pubkey) pairs for the entire epoch
    ///
    pub fn iter_unnested_schedule(&self) -> impl Iterator<Item = (u64, &Pubkey)> {
        let first_slot = self.first_slot;
        self.schedule
            .iter()
            .enumerate()
            .flat_map(move |(slot_idx, pubk)| {
                (0..4).map(move |i| {
                    let slot = first_slot + (slot_idx as u64 * 4) + i;
                    (slot, pubk)
                })
            })
    }
}

pub fn unnest_rpc_get_leader_schedule_resp<'a, 'iter>(
    slot: u64,
    resp: impl IntoIterator<Item = (&'iter String, &'iter Vec<usize>)>,
) -> CompactSortedSchedule
where
    'a: 'iter,
{
    let mut ret = Vec::with_capacity(DEFAULT_SLOTS_PER_EPOCH as usize / 4);

    ret.resize(ret.capacity(), Pubkey::default());

    for (pubkey_str, sorted_slot_idx) in resp {
        let pubkey = match Pubkey::from_str(pubkey_str.as_str()) {
            Ok(pubkey) => pubkey,
            Err(err) => {
                tracing::warn!(
                    "Skipping leader schedule entry with invalid pubkey {:?}: {:?}",
                    pubkey_str,
                    err
                );
                continue;
            }
        };
        sorted_slot_idx
            .iter()
            .filter(|s| *s % 4 == 0)
            .map(|s| s / 4)
            .for_each(|slot_idx| {
                ret[slot_idx] = pubkey;
            });
    }
    let epoch = slot / DEFAULT_SLOTS_PER_EPOCH;

    CompactSortedSchedule {
        first_slot: epoch * DEFAULT_SLOTS_PER_EPOCH,
        schedule: ret,
    }
}

#[async_trait::async_trait]
pub trait ScheduleExt {
    ///
    /// Get the leader schedule for the epoch containing `slot_ctx`,
    /// unnesting the RPC response into a [`CompactSortedSchedule`].
    ///
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError>;
}

#[async_trait::async_trait]
impl ScheduleExt for RpcClient {
    async fn get_unnested_leader_schedule(
        &self,
        slot_ctx: Option<u64>,
    ) -> Result<Option<CompactSortedSchedule>, client_error::ClientError> {
        let referenced_slot = match slot_ctx {
            Some(slot) => slot,
            None => {
                let info = self.get_epoch_info().await?;
                info.absolute_slot
            }
        };

        Ok(self
            .get_leader_schedule(Some(referenced_slot))
            .await?
            .map(|nested| unnest_rpc_get_leader_schedule_resp(referenced_slot, nested.iter())))
    }
}

#[derive(Debug)]
struct InnerManagedLeaderSchedule {
    double_buffer: [CompactSortedSchedule; 2],
    fail: AtomicBool,
}

///
/// A managed leader schedule that automatically updates as epochs progress.
///
/// See [`spawn_managed_leader_schedule`] for spawning the background update task.
///
/// # Safety
///
/// This struct uses internal synchronization to allow concurrent access from multiple tasks.
///
/// # Clone
///
/// You can clone (cheaply) the `ManagedLeaderSchedule` to share it across multiple tasks.
///
#[derive(Clone)]
pub struct ManagedLeaderSchedule {
    inner: Arc<RwLock<InnerManagedLeaderSchedule>>,
}

///
/// Error indicating that the AutoLeaderSchedule background update task has failed.
///
#[derive(Debug, thiserror::Error)]
#[error("auto leader schedule poisoned")]
pub struct PoisonError;

impl ManagedLeaderSchedule {
    #[cfg(test)]
    pub(crate) fn new_for_tests(first_slot: u64, schedule: Vec<Pubkey>) -> Self {
        let current = CompactSortedSchedule {
            first_slot,
            schedule: schedule.clone(),
        };
        let next = CompactSortedSchedule {
            first_slot: first_slot + DEFAULT_SLOTS_PER_EPOCH,
            schedule,
        };
        Self {
            inner: Arc::new(RwLock::new(InnerManagedLeaderSchedule {
                double_buffer: [current, next],
                fail: AtomicBool::new(false),
            })),
        }
    }

    fn get_leader_from_schedules(
        schedules: &InnerManagedLeaderSchedule,
        slot: u64,
    ) -> Option<Pubkey> {
        let schedule = if slot >= schedules.double_buffer[1].first_slot {
            &schedules.double_buffer[1]
        } else {
            &schedules.double_buffer[0]
        };
        schedule.get(&slot).cloned()
    }

    ///
    /// Get the leader for a given slot.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(Pubkey))` if the leader for the slot is found.
    /// - `Ok(None)` if the slot is out of range of the current schedules.
    /// - `Err(PoisonError)` if the background update task has failed.
    ///
    /// # Errors
    ///
    /// Returns `PoisonError` if the background update task has failed.
    ///
    pub fn get_leader(&self, slot: u64) -> Result<Option<Pubkey>, PoisonError> {
        let Ok(schedules) = self.inner.read() else {
            return Err(PoisonError);
        };
        // Relaxed ordering is sufficient here since fail does not protect any data.
        // We already use RwLock to protect the double_buffer data.
        if schedules.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PoisonError);
        }
        Ok(Self::get_leader_from_schedules(&schedules, slot))
    }

    ///
    /// Get leaders for multiple slots while taking the schedule read lock only once.
    ///
    /// This is useful on the transaction submission path where the current and next
    /// leader boundary are checked together near a slot boundary.
    ///
    pub fn get_leaders_for_slots<const N: usize>(
        &self,
        slots: [u64; N],
    ) -> Result<[Option<Pubkey>; N], PoisonError> {
        let Ok(schedules) = self.inner.read() else {
            return Err(PoisonError);
        };
        if schedules.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PoisonError);
        }
        Ok(slots.map(|slot| Self::get_leader_from_schedules(&schedules, slot)))
    }

    ///
    /// Gets unique leaders for the current slot and upcoming slots in one schedule read.
    ///
    /// `fanout_slots` follows Solana TPU client's semantics: the range starts at
    /// `current_slot` and advances by the four-slot leader stride until the window ends.
    ///
    pub fn get_unique_leaders_for_slot_fanout(
        &self,
        current_slot: u64,
        fanout_slots: u64,
    ) -> Result<Vec<Pubkey>, PoisonError> {
        let Ok(schedules) = self.inner.read() else {
            return Err(PoisonError);
        };
        if schedules.fail.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PoisonError);
        }

        let leader_count = fanout_slots.div_ceil(4) as usize;
        let mut leaders = Vec::with_capacity(leader_count);
        let end_slot = current_slot.saturating_add(fanout_slots);
        for leader_slot in (current_slot..end_slot).step_by(4) {
            if let Some(leader) = Self::get_leader_from_schedules(&schedules, leader_slot)
                && !leaders.contains(&leader)
            {
                leaders.push(leader);
            }
        }
        Ok(leaders)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnManagedLeaderScheduleError {
    #[error(transparent)]
    Client(#[from] client_error::ClientError),
    #[error("leader schedule unavailable for slot context {0:?}")]
    LeaderScheduleUnavailable(Option<u64>),
}

async fn auto_leader_schedule_loop(
    config: ManagedLeaderScheduleConfig,
    shared: Arc<RwLock<InnerManagedLeaderSchedule>>,
    rpc_client: Arc<RpcClient>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    let initial = match shared.read() {
        Ok(shared) => shared.double_buffer.clone(),
        Err(err) => {
            tracing::error!("AutoLeaderSchedule: lock poisoned on startup: {:?}", err);
            return;
        }
    };

    let mut current_epoch = initial[0].first_slot / DEFAULT_SLOTS_PER_EPOCH;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(config.check_interval) => { }
            _ = cancellation_token.cancelled() => {
                tracing::info!("AutoLeaderSchedule: cancellation requested, exiting loop");
                break;
            }
        }

        let epoch = match rpc_client.get_epoch_info().await {
            Ok(epoch_info) => epoch_info.epoch,
            Err(err) => {
                tracing::error!(
                    "AutoLeaderSchedule: failed to fetch epoch info; keeping previous schedule: {:?}",
                    err
                );
                continue;
            }
        };

        if epoch == current_epoch {
            tracing::debug!("AutoLeaderSchedule: still in epoch {}", current_epoch);

            // Only fetch the next schedule if we're still in the same epoch
            // Making sure we have the freshest schedule ready when we transition
            let next_epoch_first_slot = (current_epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;
            let next_schedule = match rpc_client
                .get_unnested_leader_schedule(Some(next_epoch_first_slot))
                .await
            {
                Ok(Some(next_schedule)) => next_schedule,
                Ok(None) => {
                    tracing::warn!(
                        "AutoLeaderSchedule: next schedule unavailable for first slot {}",
                        next_epoch_first_slot
                    );
                    continue;
                }
                Err(err) => {
                    tracing::error!(
                        "AutoLeaderSchedule: failed to fetch next schedule; keeping previous schedule: {:?}",
                        err
                    );
                    continue;
                }
            };
            match shared.write() {
                Ok(mut schedules) => {
                    schedules.double_buffer[1] = next_schedule;
                }
                Err(err) => {
                    tracing::error!("AutoLeaderSchedule: lock poisoned: {:?}", err);
                    return;
                }
            }

            continue;
        } else {
            tracing::info!(
                "AutoLeaderSchedule: detected epoch change {} -> {}",
                current_epoch,
                epoch
            );
            let first_slot_current_epoch = epoch * DEFAULT_SLOTS_PER_EPOCH;
            let next_epoch_first_slot = (epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;

            let current_schedule_fut =
                rpc_client.get_unnested_leader_schedule(Some(first_slot_current_epoch));

            let next_schedule_fut =
                rpc_client.get_unnested_leader_schedule(Some(next_epoch_first_slot));

            let (current_schedule, next_schedule) =
                join(current_schedule_fut, next_schedule_fut).await;
            let current_schedule = match current_schedule {
                Ok(Some(current_schedule)) => current_schedule,
                Ok(None) => {
                    tracing::warn!(
                        "AutoLeaderSchedule: current schedule unavailable for first slot {}",
                        first_slot_current_epoch
                    );
                    continue;
                }
                Err(err) => {
                    tracing::error!(
                        "AutoLeaderSchedule: failed to fetch current schedule; keeping previous schedule: {:?}",
                        err
                    );
                    continue;
                }
            };
            let next_schedule = match next_schedule {
                Ok(Some(next_schedule)) => next_schedule,
                Ok(None) => {
                    tracing::warn!(
                        "AutoLeaderSchedule: next schedule unavailable for first slot {}",
                        next_epoch_first_slot
                    );
                    continue;
                }
                Err(err) => {
                    tracing::error!(
                        "AutoLeaderSchedule: failed to fetch next schedule; keeping previous schedule: {:?}",
                        err
                    );
                    continue;
                }
            };

            match shared.write() {
                Ok(mut schedules) => {
                    schedules.double_buffer = [current_schedule, next_schedule];
                    current_epoch = epoch;
                }
                Err(err) => {
                    tracing::error!("AutoLeaderSchedule: lock poisoned: {:?}", err);
                    return;
                }
            }
        }
    }
}

///
/// Configuration for spawning a managed leader schedule.
///
/// See [`spawn_managed_leader_schedule`].
#[derive(Debug, Clone, Deserialize)]
pub struct ManagedLeaderScheduleConfig {
    /// How long to wait before checking for a new epoch schedule
    #[serde(
        with = "humantime_serde",
        default = "ManagedLeaderScheduleConfig::default_check_interval"
    )]
    pub check_interval: std::time::Duration,
}

impl ManagedLeaderScheduleConfig {
    ///
    /// Default check interval duration.
    ///
    pub fn default_check_interval() -> std::time::Duration {
        DEFAULT_AUTO_LEADER_SCHEDULE_CHECK_INTERVAL
    }
}

impl Default for ManagedLeaderScheduleConfig {
    fn default() -> Self {
        Self {
            check_interval: Self::default_check_interval(),
        }
    }
}

///
/// Spawn a managed leader schedule that automatically updates as epochs progress.
///
pub async fn spawn_managed_leader_schedule(
    rpc_client: Arc<RpcClient>,
    config: ManagedLeaderScheduleConfig,
) -> Result<(ManagedLeaderSchedule, JoinHandle<()>), SpawnManagedLeaderScheduleError> {
    let initial_schedule = rpc_client.get_unnested_leader_schedule(None).await?.ok_or(
        SpawnManagedLeaderScheduleError::LeaderScheduleUnavailable(None),
    )?;

    let current_epoch = initial_schedule.first_slot / DEFAULT_SLOTS_PER_EPOCH;
    let next_epoch_first_slot = (current_epoch + 1) * DEFAULT_SLOTS_PER_EPOCH;

    let next_schedule = rpc_client
        .get_unnested_leader_schedule(Some(next_epoch_first_slot))
        .await?
        .ok_or(SpawnManagedLeaderScheduleError::LeaderScheduleUnavailable(
            Some(next_epoch_first_slot),
        ))?;

    let shared = Arc::new(RwLock::new(InnerManagedLeaderSchedule {
        double_buffer: [initial_schedule, next_schedule],
        fail: AtomicBool::new(false),
    }));

    let shared_clone = shared.clone();
    let cancellation_token = tokio_util::sync::CancellationToken::new();
    let loop_ct = cancellation_token.clone();
    let jh = tokio::spawn(async move {
        auto_leader_schedule_loop(config, shared_clone, rpc_client, loop_ct).await;
    });

    Ok((ManagedLeaderSchedule { inner: shared }, jh))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::distr::{Distribution, weighted::WeightedIndex},
        solana_clock::DEFAULT_SLOTS_PER_EPOCH,
        solana_pubkey::Pubkey,
        std::{
            collections::BTreeMap,
            sync::{Arc, RwLock, atomic::AtomicBool},
        },
    };

    fn exponential_distribution(n: usize, base: f64, r: f64) -> Vec<u64> {
        // returns a vector of stake weights
        (0..n).map(|i| (base * r.powi(i as i32)) as u64).collect()
    }

    #[test]
    fn test_unnest_rpc_get_leader_schedule_resp() {
        let validator_rounds = DEFAULT_SLOTS_PER_EPOCH as usize / 4;
        const N: usize = 100;
        let stakes = exponential_distribution(N, 1_000_000.0, 0.97);
        let dist = WeightedIndex::new(&stakes).unwrap();

        let validator_keys = (0..N)
            .map(|_| Pubkey::new_unique())
            .collect::<Vec<Pubkey>>();

        let mut rng = rand::rng();
        let mut nested_schedule: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for round in 0..validator_rounds {
            let chosen_idx = dist.sample(&mut rng);
            let chosen_key = &validator_keys[chosen_idx];
            let entries = nested_schedule.entry(chosen_key.to_string()).or_default();

            for i in 0..4 {
                let slot_idx = round * 4 + i;
                entries.push(slot_idx);
            }
        }

        let compact_schedule =
            super::unnest_rpc_get_leader_schedule_resp(0, nested_schedule.iter());

        assert!(compact_schedule.first_slot == 0);

        let mut actual: BTreeMap<String, Vec<usize>> = Default::default();

        for (slot, pubkey) in compact_schedule.iter_unnested_schedule() {
            let pubkey_str = pubkey.to_string();
            assert!(
                nested_schedule.contains_key(&pubkey_str),
                "Unknown pubkey {pubkey_str}",
            );
            // println!("Slot {} assigned to leader {}", slot, pubkey_str);
            actual.entry(pubkey_str).or_default().push(slot as usize);
        }

        // // assert keys perflectly match
        assert_eq!(nested_schedule.len(), actual.len());
        let diff = nested_schedule
            .keys()
            .filter(|k| !actual.contains_key(*k))
            .collect::<Vec<_>>();

        assert!(diff.is_empty(), "Mismatched keys: {diff:?}");
        assert_eq!(nested_schedule, actual);
    }

    #[test]
    fn get_leaders_for_slots_matches_individual_lookup() {
        let leader_a = Pubkey::new_unique();
        let leader_b = Pubkey::new_unique();
        let current = CompactSortedSchedule {
            first_slot: 0,
            schedule: vec![leader_a, leader_b],
        };
        let next = CompactSortedSchedule {
            first_slot: DEFAULT_SLOTS_PER_EPOCH,
            schedule: vec![Pubkey::new_unique()],
        };
        let managed = ManagedLeaderSchedule {
            inner: Arc::new(RwLock::new(InnerManagedLeaderSchedule {
                double_buffer: [current, next],
                fail: AtomicBool::new(false),
            })),
        };

        let batched = managed
            .get_leaders_for_slots([0, 4])
            .expect("batched lookup");

        assert_eq!(batched[0], managed.get_leader(0).expect("slot 0"));
        assert_eq!(batched[1], managed.get_leader(4).expect("slot 4"));
        assert_eq!(batched, [Some(leader_a), Some(leader_b)]);
    }

    #[test]
    fn fanout_leader_lookup_avoids_hash_table_on_send_path() {
        let source = include_str!("schedule.rs");
        let hash_set_capacity = format!("{}Set::with_capacity", "Hash");

        assert!(
            !source.contains(&hash_set_capacity),
            "leader fanout lookup is on the transaction send path; use bounded linear dedupe instead of allocating a hash table"
        );
    }
}
