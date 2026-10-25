use super::*;
use std::sync::atomic::AtomicUsize;

struct CountingContactInfo(AtomicUsize);

#[tokio::test]
async fn expired_deadline_does_not_poll_an_immediately_ready_write() {
    let polled = AtomicBool::new(false);
    let result = before_send_deadline(
        Instant::now() - Duration::from_millis(1),
        &CancellationToken::new(),
        async {
            polled.store(true, Ordering::Relaxed);
            Ok(())
        },
    )
    .await;
    assert!(matches!(result, Err(SendTxError::DeadlineExceeded)));
    assert!(!polled.load(Ordering::Relaxed));
}
impl LeaderTpuInfoService for CountingContactInfo {
    fn get_quic_tpu_socket_addr(&self, _: &Pubkey) -> Option<SocketAddr> {
        self.0.fetch_add(1, Ordering::Relaxed);
        None
    }
    fn get_quic_tpu_fwd_socket_addr(&self, peer: &Pubkey) -> Option<SocketAddr> {
        self.get_quic_tpu_socket_addr(peer)
    }
}

#[tokio::test]
async fn address_watches_run_only_when_polled_by_the_driver() {
    let contacts = Arc::new(CountingContactInfo(AtomicUsize::new(0)));
    let mut watcher = RemotePeerAddrWatcher::new(
        Duration::from_millis(10),
        TpuPortKind::default(),
        contacts.clone(),
    );
    let peer = Pubkey::new_unique();
    watcher.register_watch(peer, "127.0.0.1:9000".parse().unwrap());
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        contacts.0.load(Ordering::Relaxed),
        0,
        "watcher must not spawn an unowned task"
    );
    let changes = tokio::time::timeout(Duration::from_secs(1), watcher.notified())
        .await
        .unwrap();
    assert_eq!(changes, HashSet::from([peer]));
    watcher.register_watch(peer, "127.0.0.1:9000".parse().unwrap());
    watcher.forget(peer);
    let count = contacts.0.load(Ordering::Relaxed);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), watcher.notified())
            .await
            .is_err()
    );
    assert_eq!(
        contacts.0.load(Ordering::Relaxed),
        count,
        "forgotten peers must not be polled"
    );
}
