//! Generated portable Organization Provisioning worker contract.

include!("generated.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_role_is_separate_and_bounded() {
        assert_eq!(CAPABILITY_ID, "lenso.organization-provisioning-worker@1");
        assert_eq!(PROCESS_DUE_OPERATION, "process_due");
    }
}
