//! Direct admission to published workers; no connection creation, retry or relay.
use {
    crate::core::{SendTxError, TpuSenderTxn},
    arc_swap::ArcSwap,
    bytes::Bytes,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        future::Future,
        net::SocketAddr,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll},
        time::Instant,
    },
    tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum PreparedAdmissionError {
    #[error("peer is not prepared")]
    Unavailable,
    #[error("prepared capacity is full")]
    Capacity,
    #[error("request deadline expired")]
    Deadline,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedFailure {
    Deadline,
    Retired,
    ConnectionLost,
    StreamStopped,
    StreamClosed,
    ZeroRttRejected,
    WorkerStopped,
}
impl From<&SendTxError> for PreparedFailure {
    fn from(error: &SendTxError) -> Self {
        match error {
            SendTxError::DeadlineExceeded => Self::Deadline,
            SendTxError::Canceled => Self::Retired,
            SendTxError::ConnectionError(_) => Self::ConnectionLost,
            SendTxError::StreamStopped(_) => Self::StreamStopped,
            SendTxError::StreamClosed => Self::StreamClosed,
            SendTxError::ZeroRttRejected => Self::ZeroRttRejected,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedStatus {
    NotSent {
        reason: PreparedFailure,
    },
    /// Quinn accepted the complete stream locally; no peer or chain acknowledgement.
    LocallyWritten,
    /// A write was polled and could have exposed bytes. Cancellation is not rollback.
    Unknown {
        reason: PreparedFailure,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct PreparedOutcome {
    pub status: PreparedStatus,
    pub completed_at: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PreparedPeer {
    pub identity: Pubkey,
    pub address: SocketAddr,
    pub generation: u64,
}

/// Advisory samples for currently published workers, not an atomic admission
/// decision. Work includes queued and executing prepared requests; unread
/// results are separate. Retired, unpublished identities are excluded.
#[derive(Clone, Copy, Debug, Default)]
pub struct PreparedUsage {
    pub published_peers: usize,
    pub capacity: usize,
    pub queued_or_reserved: usize,
    pub work_outstanding: usize,
    pub results_outstanding: usize,
}

/// The original worker result. Dropping the receiver releases result capacity
/// but does not cancel admitted work or release its execution capacity.
pub struct PreparedSubmission {
    peer: PreparedPeer,
    result: oneshot::Receiver<PreparedOutcome>,
    result_capacity: Option<OwnedSemaphorePermit>,
}
impl PreparedSubmission {
    pub fn peer(&self) -> PreparedPeer {
        self.peer
    }
}
impl Future for PreparedSubmission {
    type Output = PreparedOutcome;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = match Pin::new(&mut self.result).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.result_capacity.take();
        Poll::Ready(result.unwrap_or_else(|_| PreparedOutcome {
            status: PreparedStatus::Unknown {
                reason: PreparedFailure::WorkerStopped,
            },
            completed_at: Instant::now(),
        }))
    }
}

struct ReadyPeer {
    info: PreparedPeer,
    sender: mpsc::Sender<TpuSenderTxn>,
    alive: Arc<AtomicBool>,
    budget: Arc<PeerBudget>,
}

// Retained across connection generations while any work or result remains.
// Otherwise replacing a connection would reset limits and allow unbounded
// retained results. The driver's bounded budget table owns these reservations.
struct PeerBudget {
    work: Arc<Semaphore>,
    results: Arc<Semaphore>,
    capacity: usize,
}
impl PeerBudget {
    fn idle(&self) -> bool {
        self.work.available_permits() == self.capacity
            && self.results.available_permits() == self.capacity
    }
}
impl ReadyPeer {
    fn available(&self) -> bool {
        self.alive.load(Ordering::Acquire) && !self.sender.is_closed()
    }
}

// Read-only liveness projection: admission does not access or retain the
// driver's cancellation tokens. The worker owns this guard before its first
// poll and while unwinding; task cancellation remains owned by the driver.
pub(crate) struct Readiness(pub(crate) Arc<AtomicBool>);
impl Drop for Readiness {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

type Published = Arc<ArcSwap<HashMap<Pubkey, Arc<ReadyPeer>>>>;

/// Cloning this handle creates no task or queue. Readiness is an owner snapshot,
/// not a delivery guarantee; an admitted request retains its result on retirement.
#[derive(Clone)]
pub struct PreparedSender {
    peers: Published,
    alive: Arc<AtomicBool>,
}
impl PreparedSender {
    /// Cold-path observation of existing queue and semaphore ownership. Adds no
    /// counters, allocations or synchronization to transaction admission.
    pub fn usage(&self) -> PreparedUsage {
        if !self.alive.load(Ordering::Acquire) {
            return PreparedUsage::default();
        }
        let mut usage = PreparedUsage::default();
        for peer in self.peers.load().values() {
            usage.published_peers += 1;
            usage.capacity += peer.budget.capacity;
            usage.queued_or_reserved += peer.sender.max_capacity() - peer.sender.capacity();
            usage.work_outstanding += peer.budget.capacity - peer.budget.work.available_permits();
            usage.results_outstanding +=
                peer.budget.capacity - peer.budget.results.available_permits();
        }
        usage
    }

    pub fn peer(&self, identity: &Pubkey) -> Option<PreparedPeer> {
        if !self.alive.load(Ordering::Acquire) {
            return None;
        }
        self.peers
            .load()
            .get(identity)
            .filter(|peer| peer.available())
            .map(|peer| peer.info)
    }

    /// Admit only to the selected identity, address and worker generation. A
    /// retired selection is refused rather than silently using a replacement.
    /// The caller supplies qualified wire bytes; no transaction parsing or size
    /// validation is repeated here. Capacity, liveness and deadline remain local.
    pub fn try_send(
        &self,
        signature: Signature,
        selected: PreparedPeer,
        wire: Bytes,
        deadline: Instant,
    ) -> Result<PreparedSubmission, PreparedAdmissionError> {
        if deadline <= Instant::now() {
            return Err(PreparedAdmissionError::Deadline);
        }
        if !self.alive.load(Ordering::Acquire) {
            return Err(PreparedAdmissionError::Unavailable);
        }
        let peers = self.peers.load();
        let peer = peers
            .get(&selected.identity)
            .filter(|peer| peer.info == selected && peer.available())
            .ok_or(PreparedAdmissionError::Unavailable)?;
        let work = peer
            .budget
            .work
            .clone()
            .try_acquire_owned()
            .map_err(|_| PreparedAdmissionError::Capacity)?;
        let result_capacity = peer
            .budget
            .results
            .clone()
            .try_acquire_owned()
            .map_err(|_| PreparedAdmissionError::Capacity)?;
        let slot = peer.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PreparedAdmissionError::Capacity,
            mpsc::error::TrySendError::Closed(_) => PreparedAdmissionError::Unavailable,
        })?;
        if !self.alive.load(Ordering::Acquire) || !peer.available() {
            return Err(PreparedAdmissionError::Unavailable);
        }
        if deadline <= Instant::now() {
            return Err(PreparedAdmissionError::Deadline);
        }
        let (outcome, result) = oneshot::channel();
        let mut tx =
            TpuSenderTxn::from_bytes(signature, selected.identity, wire).with_deadline(deadline);
        tx.completion = Some(Completion {
            outcome: Some(outcome),
            work: Some(work),
            exposed: false,
        });
        slot.send(tx);
        Ok(PreparedSubmission {
            peer: peer.info,
            result,
            result_capacity: Some(result_capacity),
        })
    }

    pub(crate) fn close(&self) {
        self.alive.store(false, Ordering::Release);
    }
}

/// Single-writer publication owned by the existing driver, not another runtime.
pub(crate) struct PreparedPeers {
    pub(crate) sender: PreparedSender,
    capacity: usize,
    generation: u64,
    budgets: HashMap<Pubkey, Arc<PeerBudget>>,
}
impl PreparedPeers {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            sender: PreparedSender {
                peers: Arc::new(ArcSwap::from_pointee(HashMap::new())),
                alive: Arc::new(AtomicBool::new(true)),
            },
            capacity,
            generation: 0,
            budgets: HashMap::new(),
        }
    }
    pub(crate) fn publish(
        &mut self,
        identity: Pubkey,
        address: SocketAddr,
        sender: mpsc::Sender<TpuSenderTxn>,
        alive: Arc<AtomicBool>,
        capacity: usize,
    ) {
        if !self.sender.alive.load(Ordering::Acquire) || !alive.load(Ordering::Acquire) {
            return;
        }
        let current = self.sender.peers.load();
        if current.get(&identity).is_some_and(|peer| peer.available()) {
            return;
        }
        // Recycle only idle, retired identities; outstanding receivers continue
        // to consume their original peer budget even when no worker is ready.
        self.budgets.retain(|identity, budget| {
            !budget.idle() || current.get(identity).is_some_and(|peer| peer.available())
        });
        let budget = if let Some(budget) = self.budgets.get(&identity) {
            budget.clone()
        } else {
            if self.budgets.len() >= self.capacity {
                return;
            }
            let budget = Arc::new(PeerBudget {
                work: Arc::new(Semaphore::new(capacity)),
                results: Arc::new(Semaphore::new(capacity)),
                capacity,
            });
            self.budgets.insert(identity, budget.clone());
            budget
        };
        let Some(generation) = self.generation.checked_add(1) else {
            return;
        };
        self.generation = generation;
        let mut peers = (**current).clone();
        peers.retain(|_, peer| peer.available());
        peers.insert(
            identity,
            Arc::new(ReadyPeer {
                info: PreparedPeer {
                    identity,
                    address,
                    generation,
                },
                sender,
                alive,
                budget,
            }),
        );
        self.sender.peers.store(Arc::new(peers));
    }
    pub(crate) fn remove(&mut self, identity: &Pubkey) {
        let current = self.sender.peers.load();
        if !current.contains_key(identity) {
            return;
        }
        let mut peers = (**current).clone();
        if let Some(peer) = peers.remove(identity) {
            peer.alive.store(false, Ordering::Release);
        }
        self.sender.peers.store(Arc::new(peers));
    }
    pub(crate) fn clear(&mut self) {
        let old = self.sender.peers.swap(Arc::new(HashMap::new()));
        for peer in old.values() {
            peer.alive.store(false, Ordering::Release);
        }
    }
}
impl Drop for PreparedPeers {
    fn drop(&mut self) {
        self.sender.close();
        self.clear();
    }
}

#[derive(Debug)]
pub(crate) struct Completion {
    outcome: Option<oneshot::Sender<PreparedOutcome>>,
    work: Option<OwnedSemaphorePermit>,
    exposed: bool,
}
impl Completion {
    pub(crate) fn mark_exposed(&mut self) {
        self.exposed = true;
    }
    pub(crate) fn finish(mut self, status: PreparedStatus) {
        self.publish(status);
    }
    pub(crate) fn fail(mut self, reason: PreparedFailure) {
        self.publish(self.failure_status(reason));
    }
    fn failure_status(&self, reason: PreparedFailure) -> PreparedStatus {
        if self.exposed {
            PreparedStatus::Unknown { reason }
        } else {
            PreparedStatus::NotSent { reason }
        }
    }
    fn publish(&mut self, status: PreparedStatus) {
        self.work.take(); // Release execution capacity before making its outcome observable.
        if let Some(outcome) = self.outcome.take() {
            let _ = outcome.send(PreparedOutcome {
                status,
                completed_at: Instant::now(),
            });
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.publish(self.failure_status(PreparedFailure::WorkerStopped));
    }
}

#[cfg(test)]
#[path = "prepared/tests.rs"]
mod tests;
