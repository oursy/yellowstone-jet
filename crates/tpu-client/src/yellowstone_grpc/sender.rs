use {
    crate::{
        config::{MAX_SEND_FANOUT_SLOTS, TpuPortKind, TpuSenderConfig},
        core::{Nothing, StakeBasedEvictionStrategy, TpuSenderTxn},
        rpc::{
            schedule::{
                ManagedLeaderSchedule, ManagedLeaderScheduleConfig,
                SpawnManagedLeaderScheduleError, spawn_managed_leader_schedule,
            },
            solana_rpc_utils::RetryRpcSender,
            stake::{RpcValidatorStakeInfoServiceConfig, rpc_validator_stake_info_service},
            tpu_info::{RpcClusterTpuQuicInfoServiceConfig, rpc_cluster_tpu_info_service},
        },
        sender::{TpuSender, TpuSenderErrorKind, create_base_tpu_client},
        slot::AtomicSlotTracker,
        yellowstone_grpc::{
            schedule::YellowstoneUpcomingLeader,
            slot_tracker::{self, YellowstoneSlotTrackerOk},
        },
    },
    bytes::Bytes,
    derive_more::Display,
    serde::Deserialize,
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client, rpc_client::RpcClientConfig,
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::{Keypair, Signature},
    solana_pubkey::Pubkey,
    solana_rpc_client::http_sender::HttpSender,
    std::{net::SocketAddr, sync::Arc},
    yellowstone_grpc_client::{
        ClientTlsConfig, GeyserGrpcBuilder, GeyserGrpcBuilderError, GeyserGrpcClient,
    },
};

pub use crate::core::{
    TpuSenderResponse, TpuSenderResponseCallback, TxDrop, TxDropReason, TxFailed, TxSent,
};

pub const DEFAULT_TPU_SENDER_CHANNEL_CAPACITY: usize = 100_000;

///
/// Configuration object for [`YellowstoneTpuSender`].
///
#[derive(Debug, Clone, Deserialize)]
pub struct YellowstoneTpuSenderConfig {
    ///
    /// TPU-Quic event-loop configuration options.
    ///
    pub tpu: TpuSenderConfig,
    ///
    /// Configuration for internal [`crate::rpc::tpu_info::RpcClusterTpuQuicInfoService`]
    ///
    pub tpu_info: RpcClusterTpuQuicInfoServiceConfig,
    ///
    /// Configuration for internal [`crate::rpc::schedule::ManagedLeaderSchedule`]
    ///
    pub schedule: ManagedLeaderScheduleConfig,
    ///
    /// Configuration for internal [`crate::rpc::stake::RpcValidatorStakeInfoService`]
    ///
    pub stake: RpcValidatorStakeInfoServiceConfig,
    ///
    /// Capacity of the internal channel used to send transactions to the TPU sender task.
    ///
    pub channel_capacity: usize,
}

impl Default for YellowstoneTpuSenderConfig {
    fn default() -> Self {
        Self {
            tpu: Default::default(),
            tpu_info: Default::default(),
            schedule: Default::default(),
            stake: Default::default(),
            channel_capacity: DEFAULT_TPU_SENDER_CHANNEL_CAPACITY,
        }
    }
}

///
/// Error cases of [`create_yellowstone_tpu_sender`].
///
#[derive(thiserror::Error, Debug)]
pub enum CreateTpuSenderError {
    ///
    /// Error caused by [`rpc_client::RpcClient`] API call.
    ///
    #[error(transparent)]
    RpcClientError(#[from] ClientError),
    ///
    /// Error caused by initial leader schedule setup.
    ///
    #[error(transparent)]
    LeaderScheduleError(#[from] SpawnManagedLeaderScheduleError),
    ///
    /// Error caused by [`yellowstone_grpc_client::GeyserGrpcClient`] API call.
    ///
    #[error(transparent)]
    YellowstoneGrpcError(#[from] yellowstone_grpc_client::GeyserGrpcClientError),
    ///
    /// Error caused by building or connecting the Yellowstone gRPC client.
    ///
    #[error(transparent)]
    YellowstoneGrpcBuilderError(#[from] GeyserGrpcBuilderError),
    ///
    /// Raised when subscribing to a remote Yellowstone gRPC Subscription ended.
    ///
    #[error("geyser client returned empty slot tracker stream")]
    GeyserSubscriptionEnded,
}

///
/// Core Yellowstone TPU sender.
///
/// The sender tracks the current Yellowstone slot and Solana leader schedule, then routes each
/// transaction to the current leader and upcoming unique leaders in the configured fanout window.
///
/// See [`create_yellowstone_tpu_sender`] for creation.
///
/// # Example
///
/// ```ignore
///
/// let my_identity = solana_keypair::read_keypair_file("/path/to/my/id.json").expect("read_keypair_file");
///
/// let NewYellowstoneTpuSender {
///     sender,
///     related_objects_jh: _,
/// } = create_yellowstone_tpu_sender(
///     Default::default(),
///     my_identity,
///     Endpoints {
///         rpc: "https://my.rpc.endpoint".to_string(),
///         grpc: "https://my.grpc.endpoint".to_string(),
///         grpc_x_token: Some("my-secret".to_string()),
///     }
/// ).await.expect("yellowstone-tpu-sender");
///
/// let rpc_client = rpc_client::RpcClient::new(
///     "https://api.mainnet-beta.solana.com",
///     CommitmentConfig::confirmed(),
/// );
///
/// let latest_blockhash = rpc_client
///     .get_latest_blockhash()
///     .await
///     .expect("get_latest_blockhash");
///
/// let instructions = vec![transfer(&identity.pubkey(), &recipient, lamports)];
/// let transaction = VersionedTransaction::try_new(
///     VersionedMessage::V0(
///         v0::Message::try_compile(&identity.pubkey(), &instructions, &[], latest_blockhash)
///             .expect("try_compile"),
///     ),
///     &[&identity],
/// )
/// .expect("try_new");
/// let signature = transaction.signatures[0];
/// tracing::info!("generate transaction {signature} with send lamports {lamports}");
/// let bincoded_txn = bincode::serialize(&transaction).expect("bincode::serialize");
/// sender
///     .send_txn(signature, bincoded_txn)
///     .await
///     .expect("send_transaction");
/// ```
///
/// # Clone
///
/// This struct is cheaply-cloneable and can be shared between threads.
#[derive(Clone)]
pub struct YellowstoneTpuSender {
    base_tpu_sender: TpuSender,
    atomic_slot_tracker: Arc<AtomicSlotTracker>,
    leader_schedule: ManagedLeaderSchedule,
    leader_tpu_info: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync>,
    tpu_port_kind: TpuPortKind,
    send_fanout_slots: u64,
}

///
/// Error case for [`YellowstoneTpuSender`]'s transaction sending API.
///
/// See [`YellowstoneTpuSender::send_txn`] for more details.
///
#[derive(Debug, Display)]
pub enum SendErrorKind {
    ///
    /// The channel between [`YellowstoneTpuSender`] and the actual tpu event-loop is closed.
    #[display("tpu sender disconnected")]
    Closed,
    ///
    /// The channel between [`YellowstoneTpuSender`] and the actual tpu event-loop is full.
    #[display("tpu sender queue full")]
    Full,
    ///
    /// The internal slot tracked closed, await [`NewYellowstoneTpuSender::related_objects_jh`] to get more information about the error.
    ///
    #[display("slot tracker disconnected")]
    SlotTrackerDisconnected,
    ///
    /// The internal managed leader schedule got poisoned, await [`NewYellowstoneTpuSender::related_objects_jh`] to get more information about the error.
    ///
    #[display("managed leader schedule disconnected")]
    ManagedLeaderScheduleDisconnected,
    ///
    /// The leader schedule does not contain an entry for the target leader slot.
    ///
    #[display("unknown leader")]
    UnknownLeader,
}

///
/// Error returned when sending a transaction with [`YellowstoneTpuSender`]'s transaction sending API.
///
#[derive(Debug, thiserror::Error)]
#[error("{kind} for transaction")]
pub struct SendError {
    ///
    /// Kind of send error.
    ///
    pub kind: SendErrorKind,
    ///
    /// The transaction that failed to be sent.
    ///
    pub txn: Bytes,
}

impl YellowstoneTpuSender {
    fn send_txn_to_leader(
        &self,
        sig: Signature,
        wire_txn: &Bytes,
        leader: Pubkey,
        mut sent_addr: Option<&mut Option<SocketAddr>>,
    ) -> Result<(), SendErrorKind> {
        let mut queued_addr = None;
        if let Some(sent_addr) = sent_addr.as_deref_mut()
            && let Some(addr) = self
                .leader_tpu_info
                .get_quic_dest_addr(&leader, self.tpu_port_kind)
        {
            if sent_addr.as_ref().is_some_and(|sent| *sent == addr) {
                return Ok(());
            }
            queued_addr = Some(addr);
        }

        let tpu_txn = TpuSenderTxn {
            tx_sig: sig,
            remote_peer: leader,
            wire: wire_txn.clone(),
        };
        self.base_tpu_sender
            .try_send_txn(tpu_txn)
            .map_err(|err| match err {
                TpuSenderErrorKind::Closed => SendErrorKind::Closed,
                TpuSenderErrorKind::Full => SendErrorKind::Full,
            })?;

        if let Some(sent_addr) = sent_addr
            && let Some(addr) = queued_addr
        {
            *sent_addr = Some(addr);
        }

        Ok(())
    }

    fn send_txn_fanout<T>(&self, sig: Signature, txn: T) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let wire_txn = Bytes::from_owner(txn);
        let current_slot = match self.atomic_slot_tracker.load() {
            Ok(slot) => slot,
            Err(_) => {
                return Err(SendError {
                    kind: SendErrorKind::SlotTrackerDisconnected,
                    txn: wire_txn,
                });
            }
        };
        let leaders = match self
            .leader_schedule
            .get_unique_leaders_for_slot_fanout(current_slot, self.send_fanout_slots)
        {
            Ok(leaders) => leaders,
            Err(_) => {
                return Err(SendError {
                    kind: SendErrorKind::ManagedLeaderScheduleDisconnected,
                    txn: wire_txn,
                });
            }
        };
        if leaders.is_empty() {
            tracing::warn!(
                "Yellowstone TPU sender missing leaders for slot {} fanout {}",
                current_slot,
                self.send_fanout_slots
            );
            return Err(SendError {
                kind: SendErrorKind::UnknownLeader,
                txn: wire_txn,
            });
        }

        let mut sent_addr = None;
        let mut first_error = None;
        let mut sent_any = false;
        let dedupe_by_addr = leaders.len() > 1;
        for leader in leaders {
            let sent_addr = dedupe_by_addr.then_some(&mut sent_addr);
            match self.send_txn_to_leader(sig, &wire_txn, leader, sent_addr) {
                Ok(()) => sent_any = true,
                Err(kind) => {
                    if first_error.is_none() {
                        first_error = Some(kind);
                    }
                }
            }
        }

        if sent_any {
            Ok(())
        } else {
            Err(SendError {
                kind: first_error.unwrap_or(SendErrorKind::UnknownLeader),
                txn: wire_txn,
            })
        }
    }

    /// Enqueues a transaction to the current and upcoming unique leaders without allocating an async future.
    pub fn try_send_txn<T>(&self, sig: Signature, txn: T) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.send_txn_fanout(sig, txn)
    }

    ///
    /// Sends a transaction to the current and upcoming unique leaders.
    ///
    /// # Arguments
    ///
    /// * `sig` - The signature identifying the transaction.
    /// * `txn` - The bincoded transaction slice to send.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the transaction was sent successfully, or a `SendError` if there was an error.
    ///
    ///
    pub async fn send_txn<T>(&self, sig: Signature, txn: T) -> Result<(), SendError>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        self.try_send_txn(sig, txn)
    }
}

///
/// Object returned when creating a new [`YellowstoneTpuSender`].
///
/// See [`create_yellowstone_tpu_sender`] for creation.
///
pub struct NewYellowstoneTpuSender {
    ///
    /// The created Yellowstone TPU sender.
    ///
    pub sender: YellowstoneTpuSender,
    ///
    /// Join handle for related background tasks.
    ///
    /// # Note
    /// Dropping this handle will not stop the TPU sender itself, but it still recommended to await it to ensure proper cleanup.
    ///
    pub related_objects_jh: tokio::task::JoinHandle<()>,
}

/// Creates a Yellowstone TPU sender with the specified configuration.
///
/// # Arguments
///
/// * `config` - [`YellowstoneTpuSenderConfig`] for the Yellowstone TPU sender.
/// * `initial_identity` - The initial identity [`Keypair`] for the TPU sender.
/// * `rpc_client` - An RPC client [`rpc_client::RpcClient`] to interact with the Solana network.
/// * `grpc_client` - A gRPC client [`GeyserGrpcClient`] to interact with the Yellowstone Geyser service.
///
/// # Returns
///
/// A [`YellowstoneTpuSender`] and its related dependency-task join handle.
///
async fn create_yellowstone_tpu_sender_with_clients<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    rpc_client: Arc<rpc_client::RpcClient>,
    grpc_client: GeyserGrpcClient,
    callback: Option<CB>,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    let (tpu_info_service, tpu_info_service_jh) =
        rpc_cluster_tpu_info_service(Arc::clone(&rpc_client), config.tpu_info).await?;

    tracing::debug!("spawned tpu info service");

    let (managed_leader_schedule, managed_leader_schedule_jh) =
        spawn_managed_leader_schedule(Arc::clone(&rpc_client), config.schedule).await?;

    tracing::debug!("spawned managed leader schedule");

    let (stake_service, stake_info_jh) =
        rpc_validator_stake_info_service(Arc::clone(&rpc_client), config.stake).await?;

    tracing::debug!("spawned stake info service");

    let YellowstoneSlotTrackerOk {
        atomic_slot_tracker,
        join_handle: slot_tracker_jh,
    } = slot_tracker::atomic_slot_tracker(grpc_client)
        .await?
        .ok_or(CreateTpuSenderError::GeyserSubscriptionEnded)?;

    tracing::debug!("spawned slot tracker service");

    // Use the stake-aware default eviction policy for the managed Yellowstone sender.
    let connection_eviction_strategy = StakeBasedEvictionStrategy {
        ..Default::default()
    };

    let leader_predictor = YellowstoneUpcomingLeader {
        slot_tracker: Arc::clone(&atomic_slot_tracker),
        managed_schedule: managed_leader_schedule.clone(),
    };
    let tpu_port_kind = config.tpu.tpu_port;
    let send_fanout_slots = config
        .tpu
        .send_fanout_slots
        .get()
        .min(MAX_SEND_FANOUT_SLOTS);
    let tpu_info_service: Arc<dyn crate::core::LeaderTpuInfoService + Send + Sync> =
        Arc::new(tpu_info_service);
    let base_tpu_sender = create_base_tpu_client(
        config.tpu,
        initial_identity,
        Arc::clone(&tpu_info_service),
        Arc::new(stake_service.clone()),
        Arc::new(connection_eviction_strategy),
        Arc::new(leader_predictor),
        callback,
        config.channel_capacity,
    )
    .await;

    tracing::debug!("created base tpu sender");

    let sender = YellowstoneTpuSender {
        base_tpu_sender,
        atomic_slot_tracker,
        leader_schedule: managed_leader_schedule,
        leader_tpu_info: Arc::clone(&tpu_info_service),
        tpu_port_kind,
        send_fanout_slots,
    };

    let handles = vec![
        tpu_info_service_jh,
        managed_leader_schedule_jh,
        stake_info_jh,
        slot_tracker_jh,
    ];
    let handle_name_vec = vec![
        "tpu-info-service",
        "managed-leader-schedule",
        "stake-info-service",
        "slot-tracker",
    ];

    Ok(NewYellowstoneTpuSender {
        sender,
        related_objects_jh: tokio::spawn(yellowstone_tpu_deps_overseer(handle_name_vec, handles)),
    })
}

///
/// Endpoints required to connect to Yellowstone services.
///
pub struct Endpoints {
    /// RPC endpoint URL.
    pub rpc: String,
    /// gRPC endpoint URL.
    pub grpc: String,
    /// Optional X-Token for authentication.
    pub grpc_x_token: Option<String>,
}

pub async fn create_yellowstone_tpu_sender(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    endpoints: Endpoints,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError> {
    create_yellowstone_tpu_sender_inner(config, initial_identity, endpoints, None::<Nothing>).await
}

pub async fn create_yellowstone_tpu_sender_with_callback<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    endpoints: Endpoints,
    callback: CB,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    create_yellowstone_tpu_sender_inner(config, initial_identity, endpoints, Some(callback)).await
}

async fn create_yellowstone_tpu_sender_inner<CB>(
    config: YellowstoneTpuSenderConfig,
    initial_identity: Keypair,
    endpoints: Endpoints,
    callback: Option<CB>,
) -> Result<NewYellowstoneTpuSender, CreateTpuSenderError>
where
    CB: TpuSenderResponseCallback,
{
    let Endpoints {
        rpc,
        grpc,
        grpc_x_token,
    } = endpoints;

    let http_sender = HttpSender::new(rpc);
    let rpc_sender = RetryRpcSender::new(http_sender, Default::default());

    let rpc_client = Arc::new(rpc_client::RpcClient::new_sender(
        rpc_sender,
        RpcClientConfig {
            commitment_config: CommitmentConfig::confirmed(),
            ..Default::default()
        },
    ));

    let grpc_client = GeyserGrpcBuilder::from_shared(grpc)?
        .x_token(grpc_x_token)?
        .tls_config(ClientTlsConfig::default().with_enabled_roots())?
        .connect()
        .await?;

    tracing::debug!("connected to rpc/grpc endpoints");

    create_yellowstone_tpu_sender_with_clients(
        config,
        initial_identity,
        rpc_client,
        grpc_client,
        callback,
    )
    .await
}

async fn yellowstone_tpu_deps_overseer(
    handle_name_vec: Vec<&'static str>,
    handles: Vec<tokio::task::JoinHandle<()>>,
) {
    // Wait for the first task to finish

    let (finished_handle, i, rest) = futures::future::select_all(handles).await;
    if finished_handle.is_err() {
        tracing::error!(
            "Yellowstone TPU sender dependency task '{}' has failed with {finished_handle:?}",
            handle_name_vec.get(i).unwrap_or(&"unknown")
        );
    } else {
        tracing::warn!(
            "Yellowstone TPU sender dependency task '{}' has finished",
            handle_name_vec.get(i).unwrap_or(&"unknown")
        );
    }

    // Abort the rest
    rest.into_iter().for_each(|jh| jh.abort());
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::core::LeaderTpuInfoService,
        std::{
            collections::HashMap,
            num::NonZeroU64,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
        },
        tokio::sync::mpsc,
    };

    struct CountingTpuInfo {
        lookup_count: AtomicUsize,
        addrs: HashMap<Pubkey, SocketAddr>,
    }

    impl CountingTpuInfo {
        fn new(addrs: HashMap<Pubkey, SocketAddr>) -> Self {
            Self {
                lookup_count: AtomicUsize::new(0),
                addrs,
            }
        }

        fn lookup_count(&self) -> usize {
            self.lookup_count.load(Ordering::Relaxed)
        }
    }

    impl LeaderTpuInfoService for CountingTpuInfo {
        fn get_quic_tpu_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.lookup_count.fetch_add(1, Ordering::Relaxed);
            self.addrs.get(leader_pubkey).copied()
        }

        fn get_quic_tpu_fwd_socket_addr(&self, leader_pubkey: &Pubkey) -> Option<SocketAddr> {
            self.lookup_count.fetch_add(1, Ordering::Relaxed);
            self.addrs.get(leader_pubkey).copied()
        }
    }

    fn test_sender(
        slot: u64,
        leaders: Vec<Pubkey>,
        tpu_info: Arc<CountingTpuInfo>,
    ) -> (YellowstoneTpuSender, mpsc::Receiver<TpuSenderTxn>) {
        test_sender_with_capacity_and_config(slot, leaders, tpu_info, 8, TpuSenderConfig::default())
    }

    fn test_sender_with_capacity(
        slot: u64,
        leaders: Vec<Pubkey>,
        tpu_info: Arc<CountingTpuInfo>,
        capacity: usize,
    ) -> (YellowstoneTpuSender, mpsc::Receiver<TpuSenderTxn>) {
        test_sender_with_capacity_and_config(
            slot,
            leaders,
            tpu_info,
            capacity,
            TpuSenderConfig::default(),
        )
    }

    fn test_sender_with_capacity_and_config(
        slot: u64,
        leaders: Vec<Pubkey>,
        tpu_info: Arc<CountingTpuInfo>,
        capacity: usize,
        config: TpuSenderConfig,
    ) -> (YellowstoneTpuSender, mpsc::Receiver<TpuSenderTxn>) {
        let (txn_tx, txn_rx) = mpsc::channel(capacity);
        let leader_tpu_info: Arc<dyn LeaderTpuInfoService + Send + Sync> = tpu_info;
        let sender = YellowstoneTpuSender {
            base_tpu_sender: TpuSender::new_for_tests(txn_tx),
            atomic_slot_tracker: Arc::new(AtomicSlotTracker::new(slot)),
            leader_schedule: ManagedLeaderSchedule::new_for_tests(0, leaders),
            leader_tpu_info,
            tpu_port_kind: config.tpu_port,
            send_fanout_slots: config.send_fanout_slots.get().min(MAX_SEND_FANOUT_SLOTS),
        };
        (sender, txn_rx)
    }

    #[tokio::test]
    async fn single_leader_send_skips_tpu_addr_lookup() {
        let current_leader = Pubkey::new_unique();
        let next_leader = Pubkey::new_unique();
        let tpu_info = Arc::new(CountingTpuInfo::new(HashMap::new()));
        let (sender, mut txn_rx) = test_sender_with_capacity_and_config(
            1,
            vec![current_leader, next_leader],
            Arc::clone(&tpu_info),
            8,
            TpuSenderConfig {
                send_fanout_slots: NonZeroU64::new(1).expect("non-zero fanout"),
                ..TpuSenderConfig::default()
            },
        );

        sender
            .send_txn(Signature::default(), vec![1_u8, 2, 3])
            .await
            .expect("send txn");

        let txn = txn_rx.try_recv().expect("one queued transaction");
        assert_eq!(txn.remote_peer, current_leader);
        assert!(txn_rx.try_recv().is_err());
        assert_eq!(tpu_info.lookup_count(), 0);
    }

    #[test]
    fn exposes_sender_factory_with_response_callback() {
        let source = include_str!("sender.rs");
        let function_name = ["create_yellowstone_tpu_sender", "with", "callback"].join("_");
        let public_factory = format!("pub async fn {function_name}");

        assert!(
            source.contains(&public_factory),
            "Myrmidon needs a public Yellowstone TPU factory that installs a response callback"
        );
    }

    #[test]
    fn single_leader_try_send_txn_uses_sync_hot_path() {
        let current_leader = Pubkey::new_unique();
        let next_leader = Pubkey::new_unique();
        let tpu_info = Arc::new(CountingTpuInfo::new(HashMap::new()));
        let (sender, mut txn_rx) = test_sender_with_capacity_and_config(
            1,
            vec![current_leader, next_leader],
            Arc::clone(&tpu_info),
            8,
            TpuSenderConfig {
                send_fanout_slots: NonZeroU64::new(1).expect("non-zero fanout"),
                ..TpuSenderConfig::default()
            },
        );

        sender
            .try_send_txn(Signature::default(), vec![1_u8, 2, 3])
            .expect("send txn");

        let txn = txn_rx.try_recv().expect("one queued transaction");
        assert_eq!(txn.remote_peer, current_leader);
        assert!(txn_rx.try_recv().is_err());
        assert_eq!(tpu_info.lookup_count(), 0);
    }

    #[tokio::test]
    async fn boundary_fanout_coalesces_duplicate_tpu_addr() {
        let current_leader = Pubkey::new_unique();
        let next_leader = Pubkey::new_unique();
        let shared_addr: SocketAddr = "127.0.0.1:9000".parse().expect("socket addr");
        let tpu_info = Arc::new(CountingTpuInfo::new(HashMap::from([
            (current_leader, shared_addr),
            (next_leader, shared_addr),
        ])));
        let (sender, mut txn_rx) =
            test_sender(2, vec![current_leader, next_leader], Arc::clone(&tpu_info));

        sender
            .send_txn(Signature::default(), vec![1_u8, 2, 3])
            .await
            .expect("send txn");

        let txn = txn_rx.try_recv().expect("one queued transaction");
        assert_eq!(txn.remote_peer, current_leader);
        assert!(txn_rx.try_recv().is_err());
        assert_eq!(tpu_info.lookup_count(), 2);
    }

    #[tokio::test]
    async fn boundary_fanout_succeeds_when_at_least_one_leader_is_enqueued() {
        let current_leader = Pubkey::new_unique();
        let next_leader = Pubkey::new_unique();
        let current_addr: SocketAddr = "127.0.0.1:9000".parse().expect("socket addr");
        let next_addr: SocketAddr = "127.0.0.1:9001".parse().expect("socket addr");
        let tpu_info = Arc::new(CountingTpuInfo::new(HashMap::from([
            (current_leader, current_addr),
            (next_leader, next_addr),
        ])));
        let (sender, mut txn_rx) =
            test_sender_with_capacity(2, vec![current_leader, next_leader], tpu_info, 1);

        sender
            .send_txn(Signature::default(), vec![1_u8, 2, 3])
            .await
            .expect("current leader enqueue should make fanout usable");

        let txn = txn_rx.try_recv().expect("current leader transaction");
        assert_eq!(txn.remote_peer, current_leader);
        assert!(txn_rx.try_recv().is_err());
    }

    #[test]
    fn default_send_fanout_targets_current_and_upcoming_leaders() {
        let current_leader = Pubkey::new_unique();
        let next_leader = Pubkey::new_unique();
        let third_leader = Pubkey::new_unique();
        let fourth_leader = Pubkey::new_unique();
        let tpu_info = Arc::new(CountingTpuInfo::new(HashMap::new()));
        let (sender, mut txn_rx) = test_sender(
            1,
            vec![current_leader, next_leader, third_leader, fourth_leader],
            tpu_info,
        );

        sender
            .try_send_txn(Signature::default(), vec![1_u8, 2, 3])
            .expect("send txn");

        let queued = [
            txn_rx.try_recv().expect("current leader transaction"),
            txn_rx.try_recv().expect("next leader transaction"),
            txn_rx.try_recv().expect("third leader transaction"),
        ];
        assert_eq!(
            queued.map(|txn| txn.remote_peer),
            [current_leader, next_leader, third_leader,]
        );
        assert!(txn_rx.try_recv().is_err());
    }
}
