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
