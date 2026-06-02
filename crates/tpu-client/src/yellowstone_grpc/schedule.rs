//! A Yellowstone-specific UpcomingLeaderPredictor implementation
//!
//! This module provides an implementation of the UpcomingLeaderPredictor trait
//! tailored for Yellowstone, utilizing gRPC and RPC services to track the current slot
//! and predict upcoming leaders.
//!
//! # Safety
//!
//! This module is designed to be thread-safe and can be shared across multiple tasks.
//!
//! # Poisoning
//!
//! The slot tracker/managed schedule used in this implementation can be poisoned if the background task
//! updating it panics or is dropped.
//!
use {
    crate::{
        core::UpcomingLeaderPredictor, rpc::schedule::ManagedLeaderSchedule,
        slot::AtomicSlotTracker,
    },
    solana_pubkey::Pubkey,
    std::sync::Arc,
};

///
/// A Yellowstone-specific implementation of UpcomingLeaderPredictor
///
/// # Safety
///
/// This struct is cheaply-cloneable and can be shared between threads.
///
#[derive(Clone)]
pub struct YellowstoneUpcomingLeader {
    pub slot_tracker: Arc<AtomicSlotTracker>,
    pub managed_schedule: ManagedLeaderSchedule,
}

impl UpcomingLeaderPredictor for YellowstoneUpcomingLeader {
    fn try_predict_next_n_leaders(&self, n: usize) -> Vec<Pubkey> {
        let slot = match self.slot_tracker.load() {
            Ok(slot) => slot,
            Err(err) => {
                tracing::warn!(
                    "Yellowstone upcoming leader prediction skipped; slot tracker unavailable: {:?}",
                    err
                );
                return Vec::new();
            }
        };
        let reminder = slot % 4;

        let next_leader_boundary = slot + (4 - reminder);
        let mut leaders = Vec::with_capacity(n);
        for leader_slot in (0..n).map(|i| next_leader_boundary + (i * 4) as u64) {
            match self.managed_schedule.get_leader(leader_slot) {
                Ok(Some(leader)) => leaders.push(leader),
                Ok(None) => {
                    tracing::warn!(
                        "Yellowstone upcoming leader prediction skipped missing leader for slot {}",
                        leader_slot
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        "Yellowstone upcoming leader prediction skipped; schedule unavailable: {:?}",
                        err
                    );
                    return Vec::new();
                }
            }
        }
        leaders
    }
}
