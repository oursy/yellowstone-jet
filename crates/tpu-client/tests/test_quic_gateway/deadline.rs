use super::*;
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(2);
// Unestablished connections drain for three Quinn PTOs (about 3 s with the
// default initial RTT). Joining must include that protocol cleanup.
const SHUTDOWN_WAIT: Duration = Duration::from_secs(5);

fn session(
    transport: quinn::TransportConfig,
    send_timeout: Duration,
) -> (
    quinn::Endpoint,
    Pubkey,
    TpuSenderSessionContext,
    mpsc::UnboundedReceiver<TpuSenderResponse>,
) {
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
        driver_tx_channel_capacity: 8,
    };
    let (results, result_rx) = mpsc::unbounded_channel();
    let session = spawner.spawn(
        identity,
        TpuSenderConfig {
            send_timeout,
            num_endpoints: NonZeroUsize::new(1).unwrap(),
            // Expiry must be terminal even when ordinary errors allow retries.
            max_send_attempt: NonZeroUsize::new(3).unwrap(),
            leader_prediction_lookahead: None,
            ..Default::default()
        },
        Arc::new(StakeBasedEvictionStrategy::default()),
        Arc::new(IgnorantLeaderPredictor),
        Some(results),
    );
    (endpoint, peer.pubkey(), session, result_rx)
}

async fn connect(endpoint: &quinn::Endpoint) -> quinn::Connection {
    timeout(WAIT, async {
        endpoint.accept().await.unwrap().await.unwrap()
    })
    .await
    .expect("loopback handshake")
}

async fn shutdown(session: TpuSenderSessionContext, endpoint: quinn::Endpoint) {
    drop(session.driver_tx_sink);
    drop(session.identity_updater);
    timeout(SHUTDOWN_WAIT, session.driver_join_handle)
        .await
        .expect("driver shutdown")
        .expect("driver task");
    endpoint.close(VarInt::from_u32(0), b"test complete");
    timeout(WAIT, endpoint.wait_idle()).await.unwrap();
}

#[tokio::test]
async fn stream_credit_timeout_is_terminal_and_does_not_leak_after_credit_release() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let (endpoint, peer, session, mut outcomes) = session(transport, Duration::from_millis(50));
    let (expired, expired_wire) = signed_v1_wire(1_233);
    session
        .driver_tx_sink
        .try_send(TpuSenderTxn::from_owned(
            expired.signatures[0],
            peer,
            expired_wire,
        ))
        .unwrap();
    let connection = connect(&endpoint).await;
    let outcome = timeout(WAIT, outcomes.recv())
        .await
        .expect("send_timeout must terminate a request waiting for stream credit")
        .unwrap();
    assert!(matches!(outcome, TpuSenderResponse::TxFailed(ref failed)
        if failed.tx_sig == expired.signatures[0] && failed.failure_reason.contains("deadline")));

    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    assert!(
        timeout(Duration::from_millis(100), connection.accept_uni())
            .await
            .is_err()
    );
    let (fresh, fresh_wire) = signed_v1_wire(4_096);
    session
        .driver_tx_sink
        .try_send(TpuSenderTxn::from_bytes(
            fresh.signatures[0],
            peer,
            Bytes::copy_from_slice(&fresh_wire),
        ))
        .unwrap();
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
    .expect("fresh request uses the same healthy connection");
    assert_eq!(received, fresh_wire);
    let outcome = timeout(WAIT, outcomes.recv()).await.unwrap().unwrap();
    assert!(
        matches!(outcome, TpuSenderResponse::TxSent(ref sent) if sent.tx_sig == fresh.signatures[0])
    );
    assert!(
        outcomes.try_recv().is_err(),
        "expiry emits one result without retry"
    );
    shutdown(session, endpoint).await;
}

#[tokio::test]
async fn partial_write_timeout_resets_stream_and_never_finishes_a_prefix() {
    let mut transport = quinn::TransportConfig::default();
    transport.stream_receive_window(VarInt::from_u32(16));
    transport.receive_window(VarInt::from_u32(64));
    let (endpoint, peer, session, mut outcomes) = session(transport, Duration::from_millis(50));
    let (transaction, wire) = signed_v1_wire(4_096);
    session
        .driver_tx_sink
        .try_send(TpuSenderTxn::from_owned(
            transaction.signatures[0],
            peer,
            wire,
        ))
        .unwrap();
    let connection = connect(&endpoint).await;
    let mut stream = timeout(WAIT, connection.accept_uni())
        .await
        .unwrap()
        .unwrap();
    // Do not read: the peer has exposed a prefix but cannot write the remainder.
    let outcome = timeout(WAIT, outcomes.recv()).await.unwrap().unwrap();
    assert!(matches!(outcome, TpuSenderResponse::TxFailed(ref failed)
        if failed.tx_sig == transaction.signatures[0] && failed.failure_reason.contains("deadline")));
    let result = timeout(WAIT, stream.read_to_end(4_096)).await.unwrap();
    assert!(
        matches!(
            result,
            Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
        ),
        "expired partial stream must reset, not finish or resume: {result:?}"
    );
    assert!(
        timeout(Duration::from_millis(100), connection.accept_uni())
            .await
            .is_err(),
        "a timed-out partial write must not be retried"
    );
    assert!(outcomes.try_recv().is_err());
    shutdown(session, endpoint).await;
}

#[tokio::test]
async fn queued_request_keeps_its_original_deadline() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let (endpoint, peer, session, mut outcomes) = session(transport, Duration::from_millis(500));
    let (first, wire) = signed_v1_wire(1_233);
    session
        .driver_tx_sink
        .try_send(TpuSenderTxn::from_owned(first.signatures[0], peer, wire))
        .unwrap();
    let connection = connect(&endpoint).await;
    let (second, wire) = signed_v1_wire(4_096);
    session
        .driver_tx_sink
        .try_send(
            TpuSenderTxn::from_owned(second.signatures[0], peer, wire)
                .with_deadline(Instant::now() + Duration::from_millis(20)),
        )
        .unwrap();
    let outcome = timeout(WAIT, outcomes.recv()).await.unwrap().unwrap();
    assert!(matches!(outcome, TpuSenderResponse::TxFailed(ref failed)
        if failed.tx_sig == first.signatures[0]));
    let outcome = timeout(Duration::from_millis(100), outcomes.recv())
        .await
        .expect("queued request must not get a fresh timeout")
        .unwrap();
    assert!(matches!(outcome, TpuSenderResponse::TxFailed(ref failed)
        if failed.tx_sig == second.signatures[0] && failed.failure_reason.contains("deadline")));
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    assert!(
        timeout(Duration::from_millis(100), connection.accept_uni())
            .await
            .is_err()
    );
    shutdown(session, endpoint).await;
}

#[tokio::test]
async fn shutdown_interrupts_stream_credit_wait_and_closes_retained_senders() {
    let mut transport = quinn::TransportConfig::default();
    transport.max_concurrent_uni_streams(VarInt::from_u32(0));
    let (endpoint, peer, session, mut outcomes) = session(transport, Duration::from_secs(30));
    let sender = session.driver_tx_sink.clone();
    let (transaction, wire) = signed_v1_wire(1_233);
    sender
        .try_send(TpuSenderTxn::from_owned(
            transaction.signatures[0],
            peer,
            wire,
        ))
        .unwrap();
    let connection = connect(&endpoint).await;
    // Keep the sender alive: shutdown must be explicit, not depend on all callers dropping.
    timeout(SHUTDOWN_WAIT, session.shutdown())
        .await
        .expect("shutdown must interrupt pending open_uni")
        .unwrap();
    assert!(sender.is_closed());
    assert!(
        outcomes.recv().await.is_some(),
        "admitted work retains an outcome"
    );
    connection.set_max_concurrent_uni_streams(VarInt::from_u32(1));
    assert!(
        !matches!(
            timeout(Duration::from_millis(100), connection.accept_uni()).await,
            Ok(Ok(_))
        ),
        "shutdown must not expose the queued transaction"
    );
    endpoint.close(VarInt::from_u32(0), b"test complete");
    timeout(WAIT, endpoint.wait_idle()).await.unwrap();
}

#[tokio::test]
async fn shutdown_reports_every_request_still_waiting_for_a_connection() {
    let (endpoint, peer, session, mut outcomes) =
        session(quinn::TransportConfig::default(), Duration::from_secs(30));
    let mut expected = HashSet::new();
    for _ in 0..3 {
        let (transaction, wire) = signed_v1_wire(1_233);
        expected.insert(transaction.signatures[0]);
        session
            .driver_tx_sink
            .try_send(TpuSenderTxn::from_owned(
                transaction.signatures[0],
                peer,
                wire,
            ))
            .unwrap();
    }
    // The endpoint never accepts a handshake; requests may still be in the inlet
    // or the driver's pending queue. Both belong to this session.
    tokio::task::yield_now().await;
    timeout(SHUTDOWN_WAIT, session.shutdown())
        .await
        .unwrap()
        .unwrap();
    let mut completed = HashSet::new();
    while let Some(outcome) = timeout(WAIT, outcomes.recv()).await.unwrap() {
        match outcome {
            TpuSenderResponse::TxDrop(dropped) => {
                for (tx, _) in dropped.dropped_tx_vec {
                    assert!(completed.insert(tx.tx_sig), "duplicate completion");
                }
            }
            other => panic!("unconnected requests must be locally dropped: {other:?}"),
        }
    }
    assert_eq!(
        completed, expected,
        "shutdown must account for every accepted request"
    );
    endpoint.close(VarInt::from_u32(0), b"test complete");
    timeout(WAIT, endpoint.wait_idle()).await.unwrap();
}

#[tokio::test]
async fn shutdown_can_join_while_an_identity_barrier_is_unreleased() {
    let (endpoint, _, session, _) =
        session(quinn::TransportConfig::default(), Duration::from_secs(30));
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    timeout(
        WAIT,
        session
            .identity_updater
            .update_identity_with_confirmation_barrier(Keypair::new(), barrier),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(SHUTDOWN_WAIT, session.shutdown())
        .await
        .expect("shutdown must not await another barrier participant")
        .unwrap();
    endpoint.close(VarInt::from_u32(0), b"test complete");
    timeout(WAIT, endpoint.wait_idle()).await.unwrap();
}
