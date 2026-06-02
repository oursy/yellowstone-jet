use {
    crate::{
        config::TpuSenderConfig,
        core::{
            ConnectionEvictionStrategy, LeaderTpuInfoService, TpuSenderDriverSpawner,
            TpuSenderResponseCallback, TpuSenderSessionContext, TpuSenderTxn,
            UpcomingLeaderPredictor, ValidatorStakeInfoService,
        },
    },
    solana_keypair::Keypair,
    std::sync::Arc,
    tokio::sync::{mpsc, mpsc::error::TrySendError},
};

///
/// A TPU sender handle that enqueues transactions into the QUIC driver.
///
/// The handle is cheap to clone and can be shared by multiple producer tasks.
///
#[derive(Clone)]
pub struct TpuSender {
    txn_tx: mpsc::Sender<TpuSenderTxn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TpuSenderErrorKind {
    #[error("driver queue full")]
    Full,
    #[error("disconnected")]
    Closed,
}

impl TpuSender {
    #[cfg(test)]
    pub(crate) fn new_for_tests(txn_tx: mpsc::Sender<TpuSenderTxn>) -> Self {
        Self { txn_tx }
    }

    ///
    /// Sends a transaction to the TPU sender task.
    ///
    pub fn try_send_txn(&self, txn: TpuSenderTxn) -> Result<(), TpuSenderErrorKind> {
        self.txn_tx.try_send(txn).map_err(|err| match err {
            TrySendError::Full(_) => TpuSenderErrorKind::Full,
            TrySendError::Closed(_) => TpuSenderErrorKind::Closed,
        })
    }
}

///
/// Base factory function to create the internal TPU sender handle.
///
/// # Arguments
///
/// * `config` - Configuration for the TPU sender.
/// * `initial_identity` - The initial identity keypair for the TPU sender.
/// * `tpu_info_service` - Service to get TPU gossip info of leaders.
/// * `stake_map_service` - Service to get stake info of validators.
/// * `eviction_strategy` - Strategy to evict connections when needed.
/// * `leader_schedule_predictor` - Predictor for upcoming leaders.
/// * `txn_capacity` - Capacity of the transaction sender channel.
///
/// # Returns
///
/// The created `TpuSender`.
///
/// Note: This function is `async` because it requires spawning async tasks for the TPU sender driver.
/// This function is a building block for higher-level TPU client factories.
///
#[allow(clippy::too_many_arguments)]
pub async fn create_base_tpu_client<CB>(
    config: TpuSenderConfig,
    initial_identity: Keypair,
    tpu_info_service: Arc<dyn LeaderTpuInfoService + Send + Sync>,
    stake_map_service: Arc<dyn ValidatorStakeInfoService + Send + Sync>,
    eviction_strategy: Arc<dyn ConnectionEvictionStrategy + Send + Sync>,
    leader_schedule_predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync>,
    callback: Option<CB>,
    txn_capacity: usize,
) -> TpuSender
where
    CB: TpuSenderResponseCallback,
{
    let spawner = TpuSenderDriverSpawner {
        stake_info_map: stake_map_service,
        leader_tpu_info_service: tpu_info_service,
        driver_tx_channel_capacity: txn_capacity,
    };

    let session = spawner.spawn(
        initial_identity,
        config,
        eviction_strategy,
        leader_schedule_predictor,
        callback,
    );

    let TpuSenderSessionContext {
        identity_updater: _,
        driver_tx_sink,
        driver_join_handle: _,
    } = session;

    TpuSender {
        txn_tx: driver_tx_sink,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{TpuSender, TpuSenderErrorKind},
        crate::core::TpuSenderTxn,
        solana_keypair::Signature,
        solana_pubkey::Pubkey,
        tokio::sync::mpsc,
    };

    fn test_sender_with_capacity(capacity: usize) -> (TpuSender, mpsc::Receiver<TpuSenderTxn>) {
        let (txn_tx, txn_rx) = mpsc::channel(capacity);
        let sender = TpuSender::new_for_tests(txn_tx);
        (sender, txn_rx)
    }

    fn test_txn() -> TpuSenderTxn {
        TpuSenderTxn::from_bytes(
            Signature::default(),
            Pubkey::new_unique(),
            bytes::Bytes::from_static(b"txn"),
        )
    }

    #[tokio::test]
    async fn send_txn_returns_full_without_waiting_when_driver_queue_is_full() {
        let (sender, _rx) = test_sender_with_capacity(1);

        sender.try_send_txn(test_txn()).expect("first send");
        let err = sender
            .try_send_txn(test_txn())
            .expect_err("full channel should fail fast");

        assert_eq!(err, TpuSenderErrorKind::Full);
    }

    #[tokio::test]
    async fn send_txn_reports_closed_when_driver_queue_is_closed() {
        let (sender, rx) = test_sender_with_capacity(1);
        drop(rx);

        let err = sender
            .try_send_txn(test_txn())
            .expect_err("closed channel should fail");

        assert_eq!(err, TpuSenderErrorKind::Closed);
    }
}
