use super::*;
use std::time::Duration;

fn setup(
    capacity: usize,
) -> (
    PreparedPeers,
    PreparedSender,
    mpsc::Receiver<TpuSenderTxn>,
    PreparedPeer,
) {
    let mut peers = PreparedPeers::new(1);
    let identity = Pubkey::new_unique();
    let (sender, receiver) = mpsc::channel(capacity);
    peers.publish(
        identity,
        "127.0.0.1:9".parse().unwrap(),
        sender,
        Arc::new(AtomicBool::new(true)),
        capacity,
    );
    let reader = peers.sender.clone();
    (
        peers,
        reader.clone(),
        receiver,
        reader.peer(&identity).unwrap(),
    )
}

#[tokio::test]
async fn usage_separates_queue_work_and_unread_results() {
    let (_peers, sender, mut receiver, peer) = setup(2);
    let outcome = sender
        .try_send(
            Signature::new_unique(),
            peer,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    let queued = sender.usage();
    assert_eq!(queued.published_peers, 1);
    assert_eq!(queued.capacity, 2);
    assert_eq!(queued.queued_or_reserved, 1);
    assert_eq!(queued.work_outstanding, 1);
    assert_eq!(queued.results_outstanding, 1);
    let tx = receiver.recv().await.unwrap();
    let active = sender.usage();
    assert_eq!(active.queued_or_reserved, 0);
    assert_eq!(active.work_outstanding, 1);
    drop(tx);
    let completed = sender.usage();
    assert_eq!(completed.work_outstanding, 0);
    assert_eq!(completed.results_outstanding, 1);
    let _ = outcome.await;
    assert_eq!(sender.usage().results_outstanding, 0);
}

#[tokio::test]
async fn trusted_wires_are_not_interpreted_by_prepared_admission() {
    let (_peers, sender, mut receiver, peer) = setup(1);
    let signature = Signature::new_unique();
    for wire in [
        Bytes::new(),
        Bytes::from(vec![0; 1_233]),
        Bytes::from(vec![0x81; 4_097]),
    ] {
        let outcome = sender
            .try_send(
                signature,
                peer,
                wire.clone(),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();
        let transaction = receiver.recv().await.unwrap();
        assert_eq!(transaction.wire, wire);
        assert_eq!(transaction.wire.as_ptr(), wire.as_ptr());
        drop(transaction);
        assert_eq!(
            outcome.await.status,
            PreparedStatus::NotSent {
                reason: PreparedFailure::WorkerStopped
            }
        );
    }
}

#[tokio::test]
async fn expired_envelopes_do_not_consume_capacity() {
    let (_peers, sender, mut receiver, peer) = setup(1);
    let signature = Signature::new_unique();
    assert!(matches!(
        sender.try_send(signature, peer, Bytes::from_static(&[0x81]), Instant::now()),
        Err(PreparedAdmissionError::Deadline)
    ));
    let result = sender
        .try_send(
            signature,
            peer,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    drop(receiver.recv().await.unwrap());
    assert_eq!(
        result.await.status,
        PreparedStatus::NotSent {
            reason: PreparedFailure::WorkerStopped
        }
    );
}

#[tokio::test]
async fn worker_panic_preserves_exposure_and_releases_execution_capacity() {
    for exposed in [false, true] {
        let (_peers, sender, mut receiver, peer) = setup(1);
        let submit = || {
            sender.try_send(
                Signature::new_unique(),
                peer,
                Bytes::from_static(&[0x81]),
                Instant::now() + Duration::from_secs(1),
            )
        };
        let outcome = submit().unwrap();
        let admitted_at = Instant::now();
        let mut tx = receiver.recv().await.unwrap();
        let task = tokio::spawn(async move {
            if exposed {
                tx.completion.as_mut().unwrap().mark_exposed();
            }
            // Keep the request on the task stack while unwinding.
            let _request = tx;
            panic!("injected worker panic");
        });
        assert!(task.await.unwrap_err().is_panic());
        let expected = if exposed {
            PreparedStatus::Unknown {
                reason: PreparedFailure::WorkerStopped,
            }
        } else {
            PreparedStatus::NotSent {
                reason: PreparedFailure::WorkerStopped,
            }
        };
        let outcome = outcome.await;
        assert_eq!(outcome.status, expected);
        assert_eq!(outcome.invoked_at.is_some(), exposed);
        if let Some(invoked_at) = outcome.invoked_at {
            assert!(invoked_at >= admitted_at && invoked_at <= outcome.completed_at);
        }
        let next = submit().expect("capacity must be released before exposing the result");
        drop(receiver.recv().await.unwrap());
        let _ = next.await;
    }
}

#[tokio::test]
async fn retired_peer_budget_cannot_be_reassigned_until_results_are_consumed() {
    let (mut peers, sender, mut receiver, peer) = setup(1);
    let old = sender
        .try_send(
            Signature::new_unique(),
            peer,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    drop(receiver.recv().await.unwrap());
    peers.remove(&peer.identity);
    let next_peer = Pubkey::new_unique();
    let (next_sender, _next_receiver) = mpsc::channel(1);
    peers.publish(
        next_peer,
        "127.0.0.1:10".parse().unwrap(),
        next_sender.clone(),
        Arc::new(AtomicBool::new(true)),
        1,
    );
    assert!(
        sender.peer(&next_peer).is_none(),
        "retirement must not reset the bounded result budget"
    );
    let _ = old.await;
    peers.publish(
        next_peer,
        "127.0.0.1:10".parse().unwrap(),
        next_sender,
        Arc::new(AtomicBool::new(true)),
        1,
    );
    assert!(sender.peer(&next_peer).is_some());
    assert_eq!(peers.budgets.len(), 1);
    drop(peers);
    assert!(
        sender.peer(&next_peer).is_none(),
        "dropping the owner must revoke all published handles"
    );
}

#[tokio::test]
async fn admission_racing_retirement_preserves_every_accepted_result() {
    let (mut peers, sender, mut receiver, peer) = setup(16);
    let barrier = Arc::new(std::sync::Barrier::new(17));
    let outcomes = std::thread::scope(|scope| {
        let tasks = (0..16)
            .map(|_| {
                let sender = sender.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    sender.try_send(
                        Signature::new_unique(),
                        peer,
                        Bytes::from_static(&[0x81]),
                        Instant::now() + Duration::from_secs(1),
                    )
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        peers.remove(&peer.identity);
        tasks
            .into_iter()
            .map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    receiver.close();
    while let Some(tx) = receiver.recv().await {
        drop(tx);
    }
    for outcome in outcomes {
        match outcome {
            Ok(outcome) => assert_eq!(
                outcome.await.status,
                PreparedStatus::NotSent {
                    reason: PreparedFailure::WorkerStopped
                }
            ),
            Err(error) => assert_eq!(error, PreparedAdmissionError::Unavailable),
        }
    }
}

#[test]
fn stale_peer_selection_cannot_admit_to_a_new_worker_generation() {
    let (mut peers, sender, _receiver, selected) = setup(1);
    let identity = selected.identity;
    let old = sender.peer(&identity).unwrap();
    peers.remove(&identity);
    let (next_sender, mut next_receiver) = mpsc::channel(1);
    peers.publish(
        identity,
        "127.0.0.1:10".parse().unwrap(),
        next_sender,
        Arc::new(AtomicBool::new(true)),
        1,
    );
    let new = sender.peer(&identity).unwrap();
    assert_ne!(old.generation, new.generation);
    assert!(matches!(
        sender.try_send(
            Signature::new_unique(),
            old,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(PreparedAdmissionError::Unavailable)
    ));
    assert!(next_receiver.try_recv().is_err());
    let _fresh = sender
        .try_send(
            Signature::new_unique(),
            new,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert!(next_receiver.try_recv().is_ok());
}

#[test]
fn worker_liveness_ends_before_the_driver_joins_it() {
    let (peers, sender, _receiver, peer) = setup(1);
    let lease = Readiness(
        peers
            .sender
            .peers
            .load()
            .get(&peer.identity)
            .unwrap()
            .alive
            .clone(),
    );
    drop(lease);
    assert!(sender.peer(&peer.identity).is_none());
    assert!(matches!(
        sender.try_send(
            Signature::new_unique(),
            peer,
            Bytes::from_static(&[0x81]),
            Instant::now() + Duration::from_secs(1)
        ),
        Err(PreparedAdmissionError::Unavailable)
    ));
}
