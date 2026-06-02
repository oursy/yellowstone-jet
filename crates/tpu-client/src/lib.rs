//!
//! Yellowstone jet-tpu-client
//!
//! This crate is port of the custom TPU-QUIC client used by [Yellowstone Jet](https://github.com/rpcpool/yellowstone-jet)
//! a subsystem of [Cascade-Marketplace](https://triton.one/cascade),
//!
//! This crate exposes a Yellowstone gRPC TPU sender for routing transactions to the current
//! Solana leader and upcoming unique leaders over QUIC.
//!
//! The internal async event-loop engine uses [quinn] and [tokio] crates to provide a high-performance QUIC-based transport protocol implementation.
//! It is designed to handle the latest Agave network changes and covers all the edge-cases observed in production usage:
//!
//! 1. Automatic leader schedule tracking and slot updates
//! 2. Automatic TPU contact-info handling:
//!    - Contact info discovery using latest gossip information from the network.
//!    - Handles TPU endpoint changes due to leader contact info flapping (e.g. Jito validators)
//! 3. Automic connection manamgent:  reconnect, connection-prediction, failures handling.
//! 4. Rescue transaction on connection dropped (e.g. due to remote peer connection eviction)
//! 5. Stake-aware TPU selection and eviction strategies.
//!
//! ## `YellowstoneTpuSender` : Smart TPU sender implementation
//!
//! This crate come with a _smart_ TPU sender implementation: [YellowstoneTpuSender](`crate::yellowstone_grpc::sender::YellowstoneTpuSender`)
//!
//! This sender implementation exposes one core sending strategy:
//! send each transaction to each unique leader in the configured slot fanout window.
//!
//! The sender automatically tracks the current slot and leader schedule, predicts upcoming
//! leaders for connection warmup, and keeps per-leader transaction queues bounded.
//!
//! ## Example
//!
//! See [repository](https://github.com/rpcpool/yellowstone-jet/blob/main/crates/tpu-client/src/bin/test-tpu-send.rs) for more examples.
//!
//! # feature-flag supports
//!
//! - **yellowstone-grpc**: Enable Yellowstone gRPC based TPU sender implementation [`crate::yellowstone_grpc`]
//!
///
/// module for top-level cnfiguration objects
///
pub mod config;
///
/// module for the internal core tpu sending driver logic
///
#[cfg(any(feature = "examples", feature = "intg-testing"))]
pub mod core;
#[cfg(not(any(feature = "examples", feature = "intg-testing")))]
#[allow(dead_code)]
mod core;
///
/// module for common tpu sender implementation
///
mod sender;

///
/// module for internal RPC utilities
///
#[cfg(any(feature = "examples", feature = "intg-testing"))]
pub mod rpc;
#[cfg(not(any(feature = "examples", feature = "intg-testing")))]
#[allow(dead_code)]
mod rpc;

///
/// module for internal slot tracking
///
mod slot;

///
/// module to host utility that utilize Yellowstone gRPC services
///
#[cfg(feature = "yellowstone-grpc")]
pub mod yellowstone_grpc;
