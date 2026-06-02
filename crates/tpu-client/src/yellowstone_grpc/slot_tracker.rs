use {
    crate::slot::AtomicSlotTracker,
    futures::{SinkExt, Stream, channel::mpsc},
    std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration},
    tokio::task::JoinHandle,
    tokio_stream::StreamExt,
    yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError, GeyserGrpcClientResult},
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterSlots, SubscribeUpdate,
            subscribe_update::UpdateOneof,
        },
        tonic::Status,
    },
};

pub(crate) const SLOT_TRACKER_DM_FILTER_NAME: &str = "jet-tpu-client";
const SLOT_TRACKER_RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
const SLOT_TRACKER_RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(5);

type SlotUpdateStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send + 'static>>;

pub struct YellowstoneSlotTrackerOk {
    pub atomic_slot_tracker: Arc<AtomicSlotTracker>,
    pub join_handle: JoinHandle<()>,
}

pub(crate) fn get_yellowstone_slot_tracker_subscribe_request() -> SubscribeRequest {
    SubscribeRequest {
        slots: HashMap::from([(
            SLOT_TRACKER_DM_FILTER_NAME.to_string(),
            SubscribeRequestFilterSlots {
                interslot_updates: Some(true),
                ..Default::default()
            },
        )]),
        ..Default::default()
    }
}

struct AutoCloseSlotTracker {
    slot_tracker: Arc<AtomicSlotTracker>,
}

impl Drop for AutoCloseSlotTracker {
    fn drop(&mut self) {
        self.slot_tracker
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[derive(Debug)]
enum SlotTrackerStreamExit {
    Ended,
    Error(Status),
}

fn mark_tracker_unavailable(slot_tracker: &AtomicSlotTracker) {
    slot_tracker
        .closed
        .store(true, std::sync::atomic::Ordering::Release);
}

fn store_slot(slot_tracker: &AtomicSlotTracker, slot: u64) {
    slot_tracker
        .slot
        .store(slot, std::sync::atomic::Ordering::Relaxed);
    slot_tracker
        .closed
        .store(false, std::sync::atomic::Ordering::Release);
}

async fn subscribe_slot_tracker_once(
    geyser_client: &mut GeyserGrpcClient,
    subscribe_request: SubscribeRequest,
) -> GeyserGrpcClientResult<SlotUpdateStream> {
    let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
    subscribe_tx
        .send(subscribe_request)
        .await
        .map_err(|err| GeyserGrpcClientError::TonicStatus(Status::internal(err.to_string())))?;
    let response = geyser_client.geyser.subscribe(subscribe_rx).await?;
    Ok(Box::pin(response.into_inner()))
}

async fn wait_for_next_slot(
    dm_slot_stream: &mut SlotUpdateStream,
) -> GeyserGrpcClientResult<Option<u64>> {
    loop {
        let Some(result) = dm_slot_stream.next().await else {
            return Ok(None);
        };

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker stream error: {:?}", err);
                return Err(GeyserGrpcClientError::TonicStatus(err));
            }
        };

        let Some(update) = response.update_oneof else {
            tracing::warn!("Yellowstone slot tracker received update without payload");
            continue;
        };

        if let UpdateOneof::Slot(subscribe_update_slot) = update {
            return Ok(Some(subscribe_update_slot.slot));
        }
    }
}

async fn process_slot_tracker_stream(
    dm_slot_stream: &mut SlotUpdateStream,
    shared: &AtomicSlotTracker,
) -> SlotTrackerStreamExit {
    let mut current_slot = shared.slot.load(std::sync::atomic::Ordering::Relaxed);
    loop {
        let Some(result) = dm_slot_stream.next().await else {
            tracing::warn!("Yellowstone slot tracker stream ended");
            return SlotTrackerStreamExit::Ended;
        };

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                tracing::error!("Yellowstone slot tracker stream error: {:?}", err);
                return SlotTrackerStreamExit::Error(err);
            }
        };

        let Some(update) = response.update_oneof else {
            tracing::warn!("Yellowstone slot tracker received update without payload");
            continue;
        };

        match update {
            UpdateOneof::Slot(subscribe_update_slot) => {
                let slot = subscribe_update_slot.slot;
                if slot <= current_slot {
                    // Ignore out-of-order or duplicate slot updates
                    continue;
                }
                current_slot = slot;
                tracing::trace!("Yellowstone slot tracker received slot update: {}", slot);
                store_slot(shared, current_slot);
            }
            _ => {
                // Ignore other updates
            }
        }
    }
}

///
/// Background task to update the AtomicSlotTracker from the Yellowstone Geyser slot stream
///
#[cfg(test)]
async fn atomic_slot_tracker_loop<S>(dm_slot_stream: S, to_drop: AutoCloseSlotTracker)
where
    S: Stream<Item = Result<SubscribeUpdate, Status>> + Unpin + Send + 'static,
{
    let shared = Arc::clone(&to_drop.slot_tracker);
    let mut boxed_stream: SlotUpdateStream = Box::pin(dm_slot_stream);
    process_slot_tracker_stream(&mut boxed_stream, &shared).await;
    drop(to_drop);
}

async fn atomic_slot_tracker_reconnect_loop(
    mut geyser_client: GeyserGrpcClient,
    mut dm_slot_stream: SlotUpdateStream,
    subscribe_request: SubscribeRequest,
    to_drop: AutoCloseSlotTracker,
) {
    let shared = Arc::clone(&to_drop.slot_tracker);
    let _close_on_exit = to_drop;
    let mut reconnect_backoff = SLOT_TRACKER_RECONNECT_INITIAL_BACKOFF;

    loop {
        match process_slot_tracker_stream(&mut dm_slot_stream, &shared).await {
            SlotTrackerStreamExit::Ended => {
                tracing::warn!("Yellowstone slot tracker stream ended; reconnecting");
            }
            SlotTrackerStreamExit::Error(err) => {
                tracing::error!(
                    "Yellowstone slot tracker stream failed; reconnecting: {:?}",
                    err
                );
            }
        }
        mark_tracker_unavailable(&shared);

        loop {
            tokio::time::sleep(reconnect_backoff).await;
            match subscribe_slot_tracker_once(&mut geyser_client, subscribe_request.clone()).await {
                Ok(stream) => {
                    tracing::info!("Yellowstone slot tracker stream reconnected");
                    dm_slot_stream = stream;
                    reconnect_backoff = SLOT_TRACKER_RECONNECT_INITIAL_BACKOFF;
                    break;
                }
                Err(err) => {
                    tracing::warn!(
                        "Yellowstone slot tracker reconnect failed; retrying in {:?}: {:?}",
                        reconnect_backoff,
                        err
                    );
                    reconnect_backoff = std::cmp::min(
                        reconnect_backoff.saturating_mul(2),
                        SLOT_TRACKER_RECONNECT_MAX_BACKOFF,
                    );
                }
            }
        }
    }
}

///
/// Creates an [`AtomicSlotTracker`] that tracks the latest slot from Yellowstone Geyser.
///
pub async fn atomic_slot_tracker(
    mut geyser_client: GeyserGrpcClient,
) -> GeyserGrpcClientResult<Option<YellowstoneSlotTrackerOk>> {
    let subscribe_request = get_yellowstone_slot_tracker_subscribe_request();
    let mut stream =
        subscribe_slot_tracker_once(&mut geyser_client, subscribe_request.clone()).await?;

    // wait for the first slot update to establish the tip
    let Some(initial_slot) = wait_for_next_slot(&mut stream).await? else {
        return Ok(None);
    };

    let shared: Arc<AtomicSlotTracker> = Arc::new(AtomicSlotTracker::new(initial_slot));
    let to_drop = AutoCloseSlotTracker {
        slot_tracker: Arc::clone(&shared),
    };
    let jh = tokio::spawn(atomic_slot_tracker_reconnect_loop(
        geyser_client,
        stream,
        subscribe_request,
        to_drop,
    ));

    Ok(Some(YellowstoneSlotTrackerOk {
        atomic_slot_tracker: shared,
        join_handle: jh,
    }))
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        std::time::Duration,
        tokio_stream::wrappers::UnboundedReceiverStream,
        yellowstone_grpc_proto::geyser::{SlotStatus, SubscribeUpdateSlot},
    };

    #[tokio::test]
    async fn test_atomic_slot_tracker_loop() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let to_drop = AutoCloseSlotTracker {
            slot_tracker: Arc::clone(&slot_tracker),
        };

        let updates = vec![
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 1,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotProcessed as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 2,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotProcessed as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 3,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotFirstShredReceived as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            }),
        ];
        let expected_slot_views = [1, 2, 3];
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let stream = UnboundedReceiverStream::new(rx);
        let handle = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

        for (i, update) in updates.into_iter().enumerate() {
            tx.send(update).expect("send update");
            tokio::time::sleep(Duration::from_millis(10)).await;
            let expected_slot = expected_slot_views[i];
            let current_slot = slot_tracker.load().expect("load");
            assert_eq!(current_slot, expected_slot);
        }

        // Drop the handle to clean up
        handle.abort();

        // Sleep a bit to ensure the drop has taken effect
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn test_it_should_poison_when_stream_empty() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(0));
        let to_drop = AutoCloseSlotTracker {
            slot_tracker: Arc::clone(&slot_tracker),
        };

        let stream = tokio_stream::iter(vec![]);
        let handle = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

        let _ = handle.await;

        assert!(
            slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn test_it_should_poison_without_panicking_when_stream_errors() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(42));
        let to_drop = AutoCloseSlotTracker {
            slot_tracker: Arc::clone(&slot_tracker),
        };

        let stream = tokio_stream::iter(vec![Err(Status::internal("h2 protocol error"))]);
        let handle = tokio::spawn(atomic_slot_tracker_loop(stream, to_drop));

        handle
            .await
            .expect("slot tracker task should not panic on stream error");

        assert!(slot_tracker.load().is_err());
        assert!(
            slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[tokio::test]
    async fn test_slot_tracker_becomes_available_again_when_new_slot_arrives() {
        let slot_tracker = Arc::new(AtomicSlotTracker::new(42));
        mark_tracker_unavailable(&slot_tracker);
        assert!(slot_tracker.load().is_err());

        let mut stream: SlotUpdateStream =
            Box::pin(tokio_stream::iter(vec![Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 43,
                    dead_error: None,
                    parent: None,
                    status: SlotStatus::SlotProcessed as i32,
                })),
                filters: vec![SLOT_TRACKER_DM_FILTER_NAME.to_string()],
                created_at: None,
            })]));

        let outcome = process_slot_tracker_stream(&mut stream, &slot_tracker).await;

        assert!(matches!(outcome, SlotTrackerStreamExit::Ended));
        assert_eq!(slot_tracker.load().expect("load"), 43);
        assert!(
            !slot_tracker
                .closed
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[test]
    fn slot_tracker_bypasses_client_autoreconnect_filter_injection() {
        let production_source = include_str!("slot_tracker.rs")
            .split("#[cfg(test)]")
            .next()
            .expect("production source");

        assert!(
            !production_source.contains(".subscribe_once("),
            "slot tracker must not use GeyserGrpcClient::subscribe_once because \
             yellowstone-grpc-client injects __autoreconnect slot/block_meta filters"
        );
        assert!(
            production_source.contains(".geyser.subscribe("),
            "slot tracker should send its one-filter request through raw geyser.subscribe"
        );
    }
}
