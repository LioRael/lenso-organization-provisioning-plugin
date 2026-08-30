//! Generated portable Organization Provisioning saga contract.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_surface_is_exact() {
        assert_eq!(CAPABILITY_ID, "lenso.organization-provisioning@1");
        assert_eq!(
            [
                GET_OPERATION,
                LIST_OPERATION,
                REQUEST_CLEANUP_OPERATION,
                RETRY_OPERATION,
                START_OPERATION,
            ],
            ["get", "list", "request_cleanup", "retry", "start"]
        );
    }
}
