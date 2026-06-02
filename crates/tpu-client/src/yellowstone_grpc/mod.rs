/// Yellowstone modules to predict upcoming leaders using gRPC+RPC services.
#[cfg(feature = "examples")]
pub mod schedule;
#[cfg(not(feature = "examples"))]
mod schedule;
/// Fully-features tpu sender using Yellowstone services.
pub mod sender;
/// Yellowstone modules to track the current slot using gRPC+RPC services.
#[cfg(feature = "examples")]
pub mod slot_tracker;
#[cfg(not(feature = "examples"))]
mod slot_tracker;
