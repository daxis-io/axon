#![no_std]

pub mod axon_common_v1 {
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

pub use axon_common_v1::{
    ObjectRef, ObjectRefOwnedView, ObjectRefView, PageInfo, PageInfoOwnedView, PageInfoView,
    PageRequest, PageRequestOwnedView, PageRequestView, ProviderAuthority, ProviderCapabilities,
    ProviderCapabilitiesOwnedView, ProviderCapabilitiesView, ProviderError, ProviderErrorCode,
    ProviderErrorOwnedView, ProviderErrorView,
};

pub fn object_ref_full_name() -> &'static str {
    <ObjectRef as buffa::MessageName>::FULL_NAME
}
