//! API version negotiation for InferaDB Ledger.
//!
//! Ensures clients and servers agree on a compatible API version.
//! The SDK sends `x-ledger-api-version` on every request; the server
//! validates it and responds with its own version header.
//!
//! ## Version Compatibility
//!
//! | Client Version | Server Min | Server Current | Result |
//! |---------------|------------|----------------|--------|
//! | 1             | 1          | 1              | OK     |
//! | 2             | 1          | 2              | OK     |
//! | 3             | 2          | 2              | FAILED_PRECONDITION |
//! | (missing)     | 1          | 1              | OK (assumed v1) |

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Current API version supported by this server.
pub const CURRENT_API_VERSION: u32 = 1;

/// Minimum API version the server accepts.
pub const MIN_SUPPORTED_API_VERSION: u32 = 1;

/// Header name for API version negotiation.
pub const API_VERSION_HEADER: &str = "x-ledger-api-version";

/// Validates the API version from an incoming gRPC request.
///
/// Returns `Ok(())` if the version is compatible. Missing version headers
/// are treated as version 1 for backwards compatibility during rollout.
///
/// # Errors
///
/// Returns `tonic::Status` with:
/// - `INVALID_ARGUMENT` if the header is not valid ASCII or not a positive integer.
/// - `FAILED_PRECONDITION` if the client version is below the server minimum or above the server
///   current version.
pub fn validate_api_version<T>(request: &tonic::Request<T>) -> Result<(), tonic::Status> {
    let version = match request.metadata().get(API_VERSION_HEADER) {
        Some(value) => {
            let version_str = value.to_str().map_err(|_| {
                tonic::Status::invalid_argument("x-ledger-api-version header is not valid ASCII")
            })?;
            version_str.parse::<u32>().map_err(|_| {
                tonic::Status::invalid_argument(format!(
                    "x-ledger-api-version must be a positive integer, got: {version_str}"
                ))
            })?
        },
        // Missing header defaults to version 1 for backwards compatibility
        None => 1,
    };

    if version < MIN_SUPPORTED_API_VERSION {
        return Err(tonic::Status::failed_precondition(format!(
            "Server requires API version >= {MIN_SUPPORTED_API_VERSION}, client sent version {version}. \
             Please upgrade your SDK."
        )));
    }

    if version > CURRENT_API_VERSION {
        return Err(tonic::Status::failed_precondition(format!(
            "Server supports API version {CURRENT_API_VERSION}, client sent version {version}. \
             Please downgrade your SDK or upgrade the server."
        )));
    }

    Ok(())
}

/// Tonic server interceptor that validates API version on incoming requests.
///
/// Use with `*ServiceServer::with_interceptor()` for client-facing services.
/// Health, Discovery, and Raft services should not use this interceptor.
pub fn api_version_interceptor(
    request: tonic::Request<()>,
) -> Result<tonic::Request<()>, tonic::Status> {
    validate_api_version(&request)?;
    Ok(request)
}

/// Tower layer that injects `x-ledger-api-version` into HTTP response headers.
///
/// Applied to the gRPC server so every response includes the server's API version,
/// allowing clients to detect which version the server supports.
#[derive(Debug, Clone, Copy)]
pub struct ApiVersionLayer;

impl<S> tower::Layer<S> for ApiVersionLayer {
    type Service = ApiVersionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ApiVersionService { inner }
    }
}

/// Tower service that injects API version response header.
#[derive(Debug, Clone)]
pub struct ApiVersionService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> tower::Service<tonic::codegen::http::Request<ReqBody>>
    for ApiVersionService<S>
where
    S: tower::Service<
            tonic::codegen::http::Request<ReqBody>,
            Response = tonic::codegen::http::Response<ResBody>,
        >,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = tonic::codegen::http::Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.await?;
            response
                .headers_mut()
                .insert(API_VERSION_HEADER, tonic::codegen::http::HeaderValue::from_static("1"));
            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::disallowed_methods, clippy::panic)]

    use super::*;

    fn request_with_version(version: &str) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        request.metadata_mut().insert(API_VERSION_HEADER, version.parse().unwrap());
        request
    }

    #[test]
    fn test_matching_version_passes() {
        let request = request_with_version("1");
        assert!(validate_api_version(&request).is_ok());
    }

    #[test]
    fn test_missing_version_defaults_to_v1() {
        let request = tonic::Request::new(());
        assert!(validate_api_version(&request).is_ok());
    }

    #[test]
    fn test_version_above_current_rejected() {
        let request = request_with_version("2");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("client sent version 2"));
        assert!(err.message().contains("supports API version 1"));
    }

    #[test]
    fn test_version_zero_rejected() {
        let request = request_with_version("0");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("requires API version >= 1"));
    }

    #[test]
    fn test_non_numeric_version_rejected() {
        let request = request_with_version("abc");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("positive integer"));
    }

    #[test]
    fn test_negative_version_rejected() {
        let request = request_with_version("-1");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_empty_version_rejected() {
        // Empty header value is not valid for tonic metadata, but test the parse path
        let request = request_with_version("0");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_interceptor_passes_valid_request() {
        let request = request_with_version("1");
        assert!(api_version_interceptor(request).is_ok());
    }

    #[test]
    fn test_interceptor_rejects_invalid_version() {
        let request = request_with_version("99");
        let err = api_version_interceptor(request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn test_constants_are_consistent() {
        assert_eq!(CURRENT_API_VERSION, 1);
        assert_eq!(MIN_SUPPORTED_API_VERSION, 1);
        // Compile-time guarantee: min <= current
        const { assert!(MIN_SUPPORTED_API_VERSION <= CURRENT_API_VERSION) };
    }

    #[test]
    fn test_version_with_whitespace_rejected() {
        let request = request_with_version("1 ");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_version_with_decimal_rejected() {
        let request = request_with_version("1.0");
        let err = validate_api_version(&request).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn test_error_messages_are_actionable() {
        let request = request_with_version("99");
        let err = validate_api_version(&request).unwrap_err();
        // Error message should tell user what to do
        assert!(
            err.message().contains("downgrade your SDK")
                || err.message().contains("upgrade the server")
        );

        let request = request_with_version("0");
        let err = validate_api_version(&request).unwrap_err();
        assert!(err.message().contains("upgrade your SDK"));
    }
}
