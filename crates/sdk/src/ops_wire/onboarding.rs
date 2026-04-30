//! Wire-based onboarding ops (email verification + registration).
//!
//! Mirrors [`super::super::ops::onboarding`]: same returned domain types, same
//! `Result<_, SdkError>` shape on the consumer side. Internal dispatch goes
//! through the wire-services-generated
//! [`UserServiceClient`](inferadb_ledger_wire_services::UserServiceClient)
//! instead of the proto-generated tonic client.
//!
//! The three RPCs that compose the onboarding flow —
//! `initiate_email_verification`, `verify_email_code`, and
//! `complete_registration` — are part of the
//! [`UserService`](inferadb_ledger_wire_services::UserServiceClient) proto
//! surface. The tonic-side `ops/onboarding.rs` is a thin module that issues
//! direct `UserServiceClient` calls; the wire-side equivalent lives in
//! [`super::user`], which already wraps the full UserService surface
//! including these three RPCs. To keep the wire dispatch layout
//! one-module-per-tonic-op (matching `ops/onboarding.rs`) without
//! duplicating the RPC bodies, this module re-exports the three onboarding
//! functions from [`super::user`].
//!
//! End-to-end coverage with a real `WireClient` + `WireServer` is deferred
//! to E.7 (TestCluster migration); the unit tests for the underlying RPC
//! bodies — request shaping, oneof projection, missing-field handling —
//! live next to their definitions in [`super::user`]. The smoke test in
//! this file pins the re-export surface so a rename in `user.rs` that
//! breaks the `ops_wire::onboarding::*` dispatch path fails locally
//! instead of silently at the F.1 wire-up.

// Re-export the three onboarding RPCs from `super::user` so callers (and
// the F.1 dispatch wire-up) can address them as
// `ops_wire::onboarding::{initiate_email_verification, verify_email_code,
// complete_registration}` — same shape as the tonic-side
// `ops::onboarding` module. The implementations themselves live in
// `super::user` because the proto surface places them on `UserService`;
// duplicating them here would fork the wire-call site.
#[allow(unused_imports)] // wired in F.1 when the connection pool flips to wire-transport
pub(crate) use super::user::{
    complete_registration, initiate_email_verification, verify_email_code,
};

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::disallowed_methods)]
mod tests {
    //! Smoke tests pinning the re-export surface. The functions
    //! themselves are exercised by the unit tests next to their
    //! definitions in [`super::super::user`]; here we only assert that
    //! the three onboarding entry points remain reachable through this
    //! module path so the F.1 dispatch wire-up doesn't break under a
    //! later rename.

    use std::sync::Arc;

    use inferadb_ledger_types::Region;
    use inferadb_ledger_wire_transport::WireClient;

    use crate::{
        error::Result,
        types::admin::{EmailVerificationCode, EmailVerificationResult, RegistrationResult},
    };

    // Type-checked references to each re-exported function. These compile
    // only if the symbols are reachable through `super::super::user::*`
    // with the exact signatures the F.1 dispatch layer will call. The
    // function pointers are coerced to a fully-spelled `fn(...) -> ...`
    // type so a future signature drift in `user.rs` (e.g. an added arg,
    // a changed return type) trips the build here rather than at the
    // F.1 wire-up.
    #[allow(clippy::type_complexity)]
    #[test]
    fn reexports_match_expected_signatures() {
        // `Arc<WireClient>` + `u128` correlation ID are the shared prefix
        // every wire op takes; the per-op trailing args mirror the
        // tonic-side `ops::onboarding` impl-block surface. Each function
        // pointer borrow is wrapped in `core::pin::Pin<Box<dyn Future>>`
        // erasure-free — coercing the `async fn` directly to a typed
        // `fn` pointer is what pins the signature.
        type InitiateFn = fn(
            Arc<WireClient>,
            u128,
            String,
            Region,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<EmailVerificationCode>> + Send>,
        >;
        type VerifyFn = fn(
            Arc<WireClient>,
            u128,
            String,
            String,
            Region,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<EmailVerificationResult>> + Send>,
        >;
        type CompleteFn = fn(
            Arc<WireClient>,
            u128,
            String,
            String,
            Region,
            String,
            String,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<RegistrationResult>> + Send>,
        >;

        // The signature-pin works by reading the function items via
        // `std::mem::size_of_val` on a zero-sized capture — no call,
        // no allocation. We only need the type-check to fire.
        fn assert_callable<F>(_f: &F) {}
        assert_callable::<InitiateFn>(&{
            let f: InitiateFn =
                |c, r, e, reg| Box::pin(super::initiate_email_verification(c, r, e, reg));
            f
        });
        assert_callable::<VerifyFn>(&{
            let f: VerifyFn =
                |c, r, e, code, reg| Box::pin(super::verify_email_code(c, r, e, code, reg));
            f
        });
        assert_callable::<CompleteFn>(&{
            let f: CompleteFn = |c, r, t, e, reg, n, o| {
                Box::pin(super::complete_registration(c, r, t, e, reg, n, o))
            };
            f
        });
    }
}
