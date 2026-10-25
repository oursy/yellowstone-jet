//! Compile-only production driver API probe; performs no network or signing work.
use yellowstone_jet_tpu_client::core::{TpuSenderDriverSpawner, TpuSenderSessionContext};

fn main() {
    let _: Option<TpuSenderDriverSpawner> = None;
    let _submit = |session: &TpuSenderSessionContext, signature, peer, wire, deadline| {
        session
            .prepared_sender()
            .try_send(signature, peer, wire, deadline)
    };
}
