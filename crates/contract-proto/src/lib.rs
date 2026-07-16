#![no_std]

pub mod axon {
    pub mod common {
        pub mod v1 {
            #![allow(
                non_camel_case_types,
                non_upper_case_globals,
                dead_code,
                unused_imports,
                unused_qualifications,
                clippy::derivable_impls,
                clippy::doc_lazy_continuation,
                clippy::match_single_binding,
                clippy::module_inception,
                clippy::uninlined_format_args
            )]

            include!("generated/axon.common.v1.mod.rs");
        }
    }

    pub mod dataaccess {
        pub mod v1 {
            #![allow(
                non_camel_case_types,
                non_upper_case_globals,
                dead_code,
                unused_imports,
                unused_qualifications,
                clippy::derivable_impls,
                clippy::doc_lazy_continuation,
                clippy::match_single_binding,
                clippy::module_inception,
                clippy::uninlined_format_args
            )]

            include!("generated/axon.dataaccess.v1.mod.rs");
        }
    }

    pub mod exec {
        pub mod v1 {
            #![allow(
                non_camel_case_types,
                non_upper_case_globals,
                dead_code,
                unused_imports,
                unused_qualifications,
                clippy::derivable_impls,
                clippy::doc_lazy_continuation,
                clippy::match_single_binding,
                clippy::module_inception,
                clippy::uninlined_format_args
            )]

            include!("generated/axon.exec.v1.mod.rs");
        }
    }

    pub mod fs {
        pub mod v1 {
            #![allow(
                non_camel_case_types,
                non_upper_case_globals,
                dead_code,
                unused_imports,
                unused_qualifications,
                clippy::derivable_impls,
                clippy::doc_lazy_continuation,
                clippy::match_single_binding,
                clippy::module_inception,
                clippy::uninlined_format_args
            )]

            include!("generated/axon.fs.v1.mod.rs");
        }
    }
}

pub mod axon_common_v1 {
    pub use crate::axon::common::v1::*;
}

pub mod axon_dataaccess_v1 {
    pub use crate::axon::dataaccess::v1::*;
}

pub mod axon_exec_v1 {
    pub use crate::axon::exec::v1::*;
}

pub mod axon_fs_v1 {
    pub use crate::axon::fs::v1::*;
}

pub use axon_common_v1::{
    ObjectRef, ObjectRefOwnedView, ObjectRefView, PageInfo, PageInfoOwnedView, PageInfoView,
    PageRequest, PageRequestOwnedView, PageRequestView, ProviderAuthority, ProviderCapabilities,
    ProviderCapabilitiesOwnedView, ProviderCapabilitiesView, ProviderError, ProviderErrorCode,
    ProviderErrorOwnedView, ProviderErrorView,
};

pub fn object_ref_full_name() -> &'static str {
    <ObjectRef as buffa::MessageName>::FULL_NAME
}

pub fn read_access_plan_full_name() -> &'static str {
    <axon_dataaccess_v1::ReadAccessPlan as buffa::MessageName>::FULL_NAME
}
