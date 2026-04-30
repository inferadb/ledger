//! Pagination helpers for slug-cursor pages.
//!
//! Provides primitives shared across `*_wire.rs` shims that paginate
//! lists by Snowflake slug:
//!
//! - [`normalize_page_size`] — clamp a requested page size to `[1, 1000]`, defaulting `0` to `100`.
//! - [`encode_page_token`] / [`decode_page_token`] — opaque page-token codec (big-endian u64 slug
//!   cursor).
//! - [`paginate_by_slug`] — sort a `(slug, T)` list by slug, apply a cursor filter, take the
//!   requested page, and return the next-page token if more items exist.
//!
//! These helpers carry no proto dependency — wire shims operate on
//! `Option<bytes::Bytes>` page tokens by converting to/from `Vec<u8>` at
//! the call boundary.

/// Clamps a requested page size to the valid range `[1, 1000]`, defaulting `0` to `100`.
pub(crate) fn normalize_page_size(requested: u32) -> usize {
    if requested == 0 { 100 } else { requested.min(1000) as usize }
}

/// Encodes a slug value as an opaque page token (big-endian `u64`).
pub(crate) fn encode_page_token(slug: u64) -> Vec<u8> {
    slug.to_be_bytes().to_vec()
}

/// Decodes an opaque page token as a big-endian `u64` slug cursor.
///
/// Returns `None` for absent or malformed tokens (non-8-byte), which
/// callers treat as "start from the beginning."
pub(crate) fn decode_page_token(token: &Option<Vec<u8>>) -> Option<u64> {
    token
        .as_ref()
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok().map(u64::from_be_bytes))
}

/// Paginates a list of `(slug, T)` pairs by slug cursor.
///
/// Sorts by slug, applies the cursor filter (items strictly after
/// `start_after`), takes `page_size` items, and returns the page items
/// plus the next page token (if more items exist).
pub(crate) fn paginate_by_slug<T>(
    mut items: Vec<(u64, T)>,
    start_after: Option<u64>,
    page_size: usize,
) -> (Vec<T>, Option<Vec<u8>>) {
    items.sort_by_key(|(slug, _)| *slug);
    if let Some(after) = start_after {
        let start = items.partition_point(|(slug, _)| *slug <= after);
        items.drain(..start);
    }
    let has_more = items.len() > page_size;
    let page: Vec<_> = items.into_iter().take(page_size).collect();
    let next_page_token =
        if has_more { page.last().map(|(slug, _)| encode_page_token(*slug)) } else { None };
    let result = page.into_iter().map(|(_, item)| item).collect();
    (result, next_page_token)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::disallowed_methods)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // encode_page_token / decode_page_token
    // -------------------------------------------------------------------------

    #[test]
    fn page_token_roundtrip() {
        let encoded = encode_page_token(42);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(42));
    }

    #[test]
    fn page_token_roundtrip_zero() {
        let encoded = encode_page_token(0);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(0));
    }

    #[test]
    fn page_token_roundtrip_max() {
        let encoded = encode_page_token(u64::MAX);
        let decoded = decode_page_token(&Some(encoded));
        assert_eq!(decoded, Some(u64::MAX));
    }

    #[test]
    fn decode_page_token_none() {
        assert_eq!(decode_page_token(&None), None);
    }

    #[test]
    fn decode_page_token_wrong_length() {
        assert_eq!(decode_page_token(&Some(vec![1, 2, 3])), None);
    }

    #[test]
    fn decode_page_token_empty() {
        assert_eq!(decode_page_token(&Some(vec![])), None);
    }

    #[test]
    fn encode_page_token_is_big_endian() {
        let bytes = encode_page_token(1);
        assert_eq!(bytes, vec![0, 0, 0, 0, 0, 0, 0, 1]);
    }

    // -------------------------------------------------------------------------
    // normalize_page_size
    // -------------------------------------------------------------------------

    #[test]
    fn normalize_page_size_zero_defaults_to_100() {
        assert_eq!(normalize_page_size(0), 100);
    }

    #[test]
    fn normalize_page_size_normal_value() {
        assert_eq!(normalize_page_size(50), 50);
    }

    #[test]
    fn normalize_page_size_max_clamped() {
        assert_eq!(normalize_page_size(5000), 1000);
    }

    #[test]
    fn normalize_page_size_exactly_1000() {
        assert_eq!(normalize_page_size(1000), 1000);
    }

    #[test]
    fn normalize_page_size_one() {
        assert_eq!(normalize_page_size(1), 1);
    }

    // -------------------------------------------------------------------------
    // paginate_by_slug
    // -------------------------------------------------------------------------

    #[test]
    fn paginate_empty() {
        let items: Vec<(u64, String)> = vec![];
        let (result, token) = paginate_by_slug(items, None, 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_single_page() {
        let items = vec![(3, "c"), (1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, None, 10);
        // Sorted by slug.
        assert_eq!(result, vec!["a", "b", "c"]);
        assert!(token.is_none());
    }

    #[test]
    fn paginate_with_more_items() {
        let items = vec![(1, "a"), (2, "b"), (3, "c")];
        let (result, token) = paginate_by_slug(items, None, 2);
        assert_eq!(result, vec!["a", "b"]);
        assert!(token.is_some());
        // Token encodes slug 2 (last item in the page).
        let cursor = decode_page_token(&token);
        assert_eq!(cursor, Some(2));
    }

    #[test]
    fn paginate_with_cursor() {
        let items = vec![(1, "a"), (2, "b"), (3, "c"), (4, "d")];
        let (result, token) = paginate_by_slug(items, Some(2), 10);
        // Items after slug 2.
        assert_eq!(result, vec!["c", "d"]);
        assert!(token.is_none());
    }

    #[test]
    fn paginate_cursor_at_end() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, Some(2), 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_cursor_beyond_end() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, Some(100), 10);
        assert!(result.is_empty());
        assert!(token.is_none());
    }

    #[test]
    fn paginate_exact_page_size() {
        let items = vec![(1, "a"), (2, "b")];
        let (result, token) = paginate_by_slug(items, None, 2);
        assert_eq!(result, vec!["a", "b"]);
        // Exactly page_size items means no more.
        assert!(token.is_none());
    }
}
