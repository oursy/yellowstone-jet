use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::timeout;
use yellowstone_jet_tpu_client::prepared::{
    PreparedAdmissionError, PreparedFailure, PreparedPeer, PreparedSender, PreparedStatus,
};

const WAIT: Duration = Duration::from_secs(2);

struct Leaders(Vec<Pubkey>);
impl UpcomingLeaderPredictor for Leaders {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        self.0.iter().copied().take(n).collect()
    }
}

struct PanicLeaderPredictor {
    leaders: Leaders,
    armed: Arc<AtomicBool>,
}
impl UpcomingLeaderPredictor for PanicLeaderPredictor {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        assert!(!self.armed.load(Ordering::Acquire), "injected driver panic");
        self.leaders.try_predict_next_n_leaders(n)
    }
}

struct Fixture {
    endpoint: quinn::Endpoint,
    peer: Pubkey,
    session: TpuSenderSessionContext,
    sender: PreparedSender,
    legacy_results: mpsc::UnboundedReceiver<TpuSenderResponse>,
}
impl Fixture {
    fn start(transport: quinn::TransportConfig, warm: bool) -> Self {
        Self::with_capacity(transport, warm, 1)
    }

    fn with_capacity(transport: quinn::TransportConfig, warm: bool, capacity: usize) -> Self {
        Self::with_driver_panic(transport, warm, capacity, None)
    }

    fn with_driver_panic(
        transport: quinn::TransportConfig,
        warm: bool,
        capacity: usize,
        panic_trigger: Option<Arc<AtomicBool>>,
    ) -> Self {
        let peer = Keypair::new();
        let identity = Keypair::new();
        let endpoint = testkit::build_validator_endpoint_with_transport(
            &peer,
            "127.0.0.1:0".parse().unwrap(),
            transport,
        );
        let spawner = TpuSenderDriverSpawner {
            stake_info_map: Arc::new(MockStakeInfoMap::constant([(identity.pubkey(), 1_000)])),
            leader_tpu_info_service: Arc::new(FakeLeaderTpuInfoService::from_iter([(
                peer.pubkey(),
                endpoint.local_addr().unwrap(),
            )])),
            driver_tx_channel_capacity: 4,
        };
        let (callback, legacy_results) = mpsc::unbounded_channel();
        let leaders = Leaders(vec![peer.pubkey()]);
        let predictor: Arc<dyn UpcomingLeaderPredictor + Send + Sync> = match panic_trigger {
            Some(armed) => Arc::new(PanicLeaderPredictor { leaders, armed }),
            None => Arc::new(leaders),
        };
        let session = spawner.spawn(
            identity,
            TpuSenderConfig {
                num_endpoints: NonZeroUsize::new(1).unwrap(),
                transaction_sender_worker_channel_capacity: capacity,
                prepared_peer_capacity: 1,
                send_timeout: Duration::from_secs(1),
                leader_prediction_lookahead: warm.then(|| NonZeroUsize::new(1).unwrap()),
                ..Default::default()
            },
            Arc::new(StakeBasedEvictionStrategy::default()),
            predictor,
            Some(callback),
        );
        let sender = session.prepared_sender();
        Self {
            endpoint,
            peer: peer.pubkey(),
            session,
            sender,
            legacy_results,
        }
    }

    async fn warm(&self) -> quinn::Connection {
        let connection = timeout(WAIT, async {
            self.endpoint.accept().await.unwrap().await.unwrap()
        })
        .await
        .unwrap();
        timeout(WAIT, async {
            while self.sender.peer(&self.peer).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("driver must publish the established worker");
        connection
    }

    // A nonexistent generation also exercises cold/closed refusal through the API.
    fn selection(&self) -> PreparedPeer {
        self.sender.peer(&self.peer).unwrap_or(PreparedPeer {
            identity: self.peer,
            address: self.endpoint.local_addr().unwrap(),
            generation: 0,
        })
    }

    async fn shutdown(self) {
        let selected = self.selection();
        timeout(Duration::from_secs(5), self.session.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(self.sender.peer(&self.peer).is_none());
        let (tx, wire) = signed_v1_wire(1_233);
        assert!(matches!(
            self.sender.try_send(
                tx.signatures[0],
                selected,
                Bytes::from(wire),
                Instant::now() + WAIT
            ),
            Err(PreparedAdmissionError::Unavailable)
        ));
        self.endpoint.close(VarInt::from_u32(0), b"test complete");
        timeout(WAIT, self.endpoint.wait_idle()).await.unwrap();
    }
}

#[tokio::test]
async fn driver_panic_drains_prepared_work_and_closes_connection_before_join_returns() {
    use futures::FutureExt;

    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(16));
    transport.receive_window(VarInt::from_u32(64));
    let panic_trigger = Arc::new(AtomicBool::new(false));
    let fixture = Fixture::with_driver_panic(transport, true, 3, Some(panic_trigger.clone()));
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let wire = Bytes::from(wire);
    let selection = fixture.selection();
    let submit = || {
        fixture
            .sender
            .try_send(
                tx.signatures[0],
                selection,
                wire.clone(),
                Instant::now() + Duration::from_secs(10),
            )
            .unwrap()
    };
    let active = submit();
    let _stream = timeout(WAIT, connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    let queued = [submit(), submit()];
    panic_trigger.store(true, Ordering::Release);
    let joined = timeout(Duration::from_secs(5), fixture.session.driver_join_handle)
        .await
        .unwrap()
        .unwrap_err();
    assert!(joined.is_panic());
    assert!(fixture.sender.peer(&fixture.peer).is_none());
    assert!(
        connection.close_reason().is_some(),
        "driver join must include QUIC close/drain"
    );
    assert_eq!(
        active
            .now_or_never()
            .expect("active worker must already be joined")
            .status,
        PreparedStatus::Unknown {
            reason: PreparedFailure::Retired
        }
    );
    for pending in queued {
        assert_eq!(
            pending
                .now_or_never()
                .expect("queued work must already be drained")
                .status,
            PreparedStatus::NotSent {
                reason: PreparedFailure::Retired
            }
        );
    }
    assert!(matches!(
        fixture
            .sender
            .try_send(tx.signatures[0], selection, wire, Instant::now() + WAIT),
        Err(PreparedAdmissionError::Unavailable)
    ));
    fixture
        .endpoint
        .close(VarInt::from_u32(0), b"panic regression complete");
    timeout(WAIT, fixture.endpoint.wait_idle()).await.unwrap();
}

#[tokio::test]
async fn cold_prepared_submission_refuses_without_starting_a_handshake() {
    let fixture = Fixture::start(quinn::TransportConfig::default(), false);
    let (tx, wire) = signed_v1_wire(1_233);
    assert!(matches!(
        fixture.sender.try_send(
            tx.signatures[0],
            fixture.selection(),
            Bytes::from(wire),
            Instant::now() + WAIT
        ),
        Err(PreparedAdmissionError::Unavailable)
    ));
    assert!(
        timeout(Duration::from_millis(50), fixture.endpoint.accept())
            .await
            .is_err()
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepared_work_and_unread_results_are_bounded_and_preserve_v1_bytes() {
    let mut fixture = Fixture::start(quinn::TransportConfig::default(), true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let wire = Bytes::from(wire);
    let submit = || {
        fixture.sender.try_send(
            tx.signatures[0],
            fixture.selection(),
            wire.clone(),
            Instant::now() + WAIT,
        )
    };
    let pending = submit().unwrap();
    assert!(matches!(submit(), Err(PreparedAdmissionError::Capacity)));
    let received = timeout(WAIT, async {
        connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(4_096)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, wire);
    assert!(
        matches!(submit(), Err(PreparedAdmissionError::Capacity)),
        "unread results remain bounded"
    );
    assert_eq!(
        timeout(WAIT, pending).await.unwrap().status,
        PreparedStatus::LocallyWritten
    );
    // Receiving a result must expose released work/result capacity immediately.
    let next = submit().unwrap();
    assert_eq!(
        timeout(WAIT, next).await.unwrap().status,
        PreparedStatus::LocallyWritten
    );
    assert!(
        fixture.legacy_results.try_recv().is_err(),
        "prepared results use their original receiver only"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepared_credit_deadline_is_not_sent_and_never_replayed() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let fixture = Fixture::start(transport, true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let pending = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            Bytes::from(wire),
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
    let outcome = timeout(WAIT, pending).await.unwrap();
    assert_eq!(
        outcome.status,
        PreparedStatus::NotSent {
            reason: PreparedFailure::Deadline
        }
    );
    assert!(outcome.invoked_at.is_none());
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    assert!(
        timeout(Duration::from_millis(100), connection.accept_uni())
            .await
            .is_err()
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepared_partial_write_timeout_is_unknown_and_not_retried() {
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(16));
    transport.receive_window(VarInt::from_u32(64));
    let fixture = Fixture::start(transport, true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let pending = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            Bytes::from(wire),
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
    let mut stream = timeout(WAIT, connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    let outcome = timeout(WAIT, pending).await.unwrap();
    assert_eq!(
        outcome.status,
        PreparedStatus::Unknown {
            reason: PreparedFailure::Deadline
        }
    );
    assert!(
        outcome
            .invoked_at
            .is_some_and(|at| at <= outcome.completed_at)
    );
    assert!(matches!(
        timeout(WAIT, stream.read_to_end(4_096)).await.unwrap(),
        Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
    ));
    assert!(
        timeout(Duration::from_millis(100), connection.accept_uni())
            .await
            .is_err()
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn prepared_invocation_time_excludes_waiting_for_stream_credit() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let fixture = Fixture::start(transport, true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(1_233);
    let mut pending = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            Bytes::from(wire.clone()),
            Instant::now() + WAIT,
        )
        .unwrap();
    assert!(
        timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    let released_at = Instant::now();
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    let received = timeout(WAIT, async {
        connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(1_233)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    let received_at = Instant::now();
    assert_eq!(received, wire);
    let outcome = timeout(WAIT, pending).await.unwrap();
    assert_eq!(outcome.status, PreparedStatus::LocallyWritten);
    let invoked_at = outcome
        .invoked_at
        .expect("original payload invocation time");
    assert!(
        invoked_at >= released_at,
        "queue/stream admission is not a send"
    );
    assert!(invoked_at <= received_at && invoked_at <= outcome.completed_at);
    fixture.shutdown().await;
}

#[tokio::test]
async fn unread_results_keep_their_capacity_across_connection_generations() {
    let fixture = Fixture::start(quinn::TransportConfig::default(), true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(1_233);
    let wire = Bytes::from(wire);
    let pending = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            wire.clone(),
            Instant::now() + WAIT,
        )
        .unwrap();
    let generation = pending.peer().generation;
    timeout(WAIT, async {
        connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(1_233)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    connection.close(VarInt::from_u32(42), b"replace connection");
    let replacement = fixture.warm().await;
    timeout(WAIT, async {
        while fixture
            .sender
            .peer(&fixture.peer)
            .is_none_or(|p| p.generation == generation)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        matches!(
            fixture.sender.try_send(
                tx.signatures[0],
                fixture.selection(),
                wire.clone(),
                Instant::now() + WAIT
            ),
            Err(PreparedAdmissionError::Capacity)
        ),
        "replacement must not create fresh capacity while old results remain unread"
    );
    assert_eq!(pending.await.status, PreparedStatus::LocallyWritten);
    let next = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            wire.clone(),
            Instant::now() + WAIT,
        )
        .unwrap();
    let received = timeout(WAIT, async {
        replacement
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(1_233)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, wire);
    assert_eq!(next.await.status, PreparedStatus::LocallyWritten);
    fixture.shutdown().await;
}

#[tokio::test]
#[expect(
    clippy::async_yields_async,
    reason = "Wait only for admission; awaiting the returned write before granting stream credit would deadlock this test."
)]
async fn dropped_receiver_does_not_free_running_work() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let fixture = Fixture::start(transport, true);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(1_233);
    let wire = Bytes::from(wire);
    let pending = fixture
        .sender
        .try_send(
            tx.signatures[0],
            fixture.selection(),
            wire.clone(),
            Instant::now() + Duration::from_millis(50),
        )
        .unwrap();
    drop(pending);
    assert!(matches!(
        fixture.sender.try_send(
            tx.signatures[0],
            fixture.selection(),
            wire.clone(),
            Instant::now() + WAIT
        ),
        Err(PreparedAdmissionError::Capacity)
    ));
    let next = timeout(WAIT, async {
        loop {
            match fixture.sender.try_send(
                tx.signatures[0],
                fixture.selection(),
                wire.clone(),
                Instant::now() + WAIT,
            ) {
                Ok(pending) => break pending,
                Err(PreparedAdmissionError::Capacity) => tokio::task::yield_now().await,
                Err(other) => panic!("unexpected refusal: {other:?}"),
            }
        }
    })
    .await
    .expect("dropped receiver must not leak execution capacity after completion");
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    assert_eq!(
        timeout(WAIT, next).await.unwrap().status,
        PreparedStatus::LocallyWritten
    );
    let received = timeout(WAIT, async {
        connection
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(1_233)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, wire);
    assert!(
        timeout(Duration::from_millis(50), connection.accept_uni())
            .await
            .is_err()
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn connection_loss_preserves_unknown_and_does_not_rescue_queued_prepared_requests() {
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(16));
    transport.receive_window(VarInt::from_u32(64));
    let fixture = Fixture::with_capacity(transport, true, 3);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let wire = Bytes::from(wire);
    let submit = || {
        fixture
            .sender
            .try_send(
                tx.signatures[0],
                fixture.selection(),
                wire.clone(),
                Instant::now() + WAIT,
            )
            .unwrap()
    };
    let active = submit();
    let _stream = timeout(WAIT, connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    let queued = [submit(), submit()];
    connection.close(VarInt::from_u32(42), b"injected connection loss");
    assert_eq!(
        timeout(WAIT, active).await.unwrap().status,
        PreparedStatus::Unknown {
            reason: PreparedFailure::ConnectionLost
        }
    );
    for pending in queued {
        assert_eq!(
            timeout(WAIT, pending).await.unwrap().status,
            PreparedStatus::NotSent {
                reason: PreparedFailure::Retired
            }
        );
    }
    let replacement = fixture.warm().await;
    assert!(
        timeout(Duration::from_millis(100), replacement.accept_uni())
            .await
            .is_err(),
        "background recovery must not rescue old prepared bytes"
    );
    let fresh = submit();
    let received = timeout(WAIT, async {
        replacement
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(4_096)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, wire);
    assert_eq!(fresh.await.status, PreparedStatus::LocallyWritten);
    fixture.shutdown().await;
}

#[tokio::test]
async fn shutdown_preserves_partial_exposure_and_retires_queued_requests() {
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(16));
    transport.receive_window(VarInt::from_u32(64));
    let fixture = Fixture::with_capacity(transport, true, 3);
    let connection = fixture.warm().await;
    let (tx, wire) = signed_v1_wire(4_096);
    let wire = Bytes::from(wire);
    let submit = || {
        fixture
            .sender
            .try_send(
                tx.signatures[0],
                fixture.selection(),
                wire.clone(),
                Instant::now() + WAIT,
            )
            .unwrap()
    };
    let active = submit();
    // Opening on the receiving side proves that the writer exposed a prefix.
    // Do not read it: flow control keeps the full transaction write pending.
    let _stream = timeout(WAIT, connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    let queued = [submit(), submit()];
    fixture.shutdown().await;
    assert_eq!(
        timeout(WAIT, active).await.unwrap().status,
        PreparedStatus::Unknown {
            reason: PreparedFailure::Retired
        }
    );
    for pending in queued {
        assert_eq!(
            timeout(WAIT, pending).await.unwrap().status,
            PreparedStatus::NotSent {
                reason: PreparedFailure::Retired
            }
        );
    }
}

#[tokio::test]
async fn stalled_peer_does_not_block_another_prepared_peer() {
    let slow_peer = Keypair::new();
    let fast_peer = Keypair::new();
    let identity = Keypair::new();
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let slow_endpoint = testkit::build_validator_endpoint_with_transport(
        &slow_peer,
        "127.0.0.1:0".parse().unwrap(),
        transport,
    );
    let fast_endpoint =
        testkit::build_validator_quic_tpu_endpoint(&fast_peer, "127.0.0.1:0".parse().unwrap());
    let spawner = TpuSenderDriverSpawner {
        stake_info_map: Arc::new(MockStakeInfoMap::constant([(identity.pubkey(), 1_000)])),
        leader_tpu_info_service: Arc::new(FakeLeaderTpuInfoService::from_iter([
            (slow_peer.pubkey(), slow_endpoint.local_addr().unwrap()),
            (fast_peer.pubkey(), fast_endpoint.local_addr().unwrap()),
        ])),
        driver_tx_channel_capacity: 4,
    };
    let session = spawner.spawn::<Nothing>(
        identity,
        TpuSenderConfig {
            num_endpoints: NonZeroUsize::new(1).unwrap(),
            transaction_sender_worker_channel_capacity: 1,
            prepared_peer_capacity: 2,
            send_timeout: Duration::from_secs(30),
            leader_prediction_lookahead: NonZeroUsize::new(2),
            ..Default::default()
        },
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(Leaders(vec![slow_peer.pubkey(), fast_peer.pubkey()])),
        None,
    );
    let (_slow, fast) = timeout(WAIT, async {
        tokio::join!(
            async { slow_endpoint.accept().await.unwrap().await.unwrap() },
            async { fast_endpoint.accept().await.unwrap().await.unwrap() }
        )
    })
    .await
    .unwrap();
    let sender = session.prepared_sender();
    timeout(WAIT, async {
        while sender.peer(&slow_peer.pubkey()).is_none()
            || sender.peer(&fast_peer.pubkey()).is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (tx, wire) = signed_v1_wire(4_096);
    let wire = Bytes::from(wire);
    let slow = sender
        .try_send(
            tx.signatures[0],
            sender.peer(&slow_peer.pubkey()).unwrap(),
            wire.clone(),
            Instant::now() + Duration::from_secs(30),
        )
        .unwrap();
    let sent = sender
        .try_send(
            tx.signatures[0],
            sender.peer(&fast_peer.pubkey()).unwrap(),
            wire.clone(),
            Instant::now() + WAIT,
        )
        .unwrap();
    let received = timeout(WAIT, async {
        fast.accept_uni()
            .await
            .unwrap()
            .read_to_end(4_096)
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, wire);
    assert_eq!(sent.await.status, PreparedStatus::LocallyWritten);
    timeout(Duration::from_secs(5), session.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        slow.await.status,
        PreparedStatus::NotSent {
            reason: PreparedFailure::Retired
        }
    );
    for endpoint in [slow_endpoint, fast_endpoint] {
        endpoint.close(VarInt::from_u32(0), b"test complete");
        timeout(WAIT, endpoint.wait_idle()).await.unwrap();
    }
}
