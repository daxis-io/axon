#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaProtocolFeatureClass {
    SupportedInBrowser,
    NativeOnly,
    TerminalUnsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaProtocolFeatureKind {
    Writer,
    ReaderWriter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeltaProtocolFeatureEnablement {
    AlwaysIfSupported,
    BoolFlag(&'static str),
    ColumnMappingMode,
    ConfigurationPrefix(&'static str),
    RowTracking,
    SchemaGeneratedColumns,
    SchemaIdentityColumns,
    SchemaInvariants,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaProtocolFeature {
    pub name: &'static str,
    pub class: DeltaProtocolFeatureClass,
    pub kind: DeltaProtocolFeatureKind,
    pub min_reader_version: i32,
    pub min_writer_version: i32,
    pub enablement: DeltaProtocolFeatureEnablement,
}

pub const KNOWN_DELTA_PROTOCOL_FEATURES: &[DeltaProtocolFeature] = &[
    DeltaProtocolFeature::writer(
        "appendOnly",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        2,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.appendOnly"),
    ),
    DeltaProtocolFeature::writer(
        "invariants",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        2,
        DeltaProtocolFeatureEnablement::SchemaInvariants,
    ),
    DeltaProtocolFeature::writer(
        "checkConstraints",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        DeltaProtocolFeatureEnablement::ConfigurationPrefix("delta.constraints."),
    ),
    DeltaProtocolFeature::writer(
        "changeDataFeed",
        DeltaProtocolFeatureClass::NativeOnly,
        4,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableChangeDataFeed"),
    ),
    DeltaProtocolFeature::writer(
        "generatedColumns",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        4,
        DeltaProtocolFeatureEnablement::SchemaGeneratedColumns,
    ),
    DeltaProtocolFeature::writer(
        "identityColumns",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        6,
        DeltaProtocolFeatureEnablement::SchemaIdentityColumns,
    ),
    DeltaProtocolFeature::writer(
        "inCommitTimestamp",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableInCommitTimestamps"),
    ),
    DeltaProtocolFeature::writer(
        "rowTracking",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::RowTracking,
    ),
    DeltaProtocolFeature::writer(
        "domainMetadata",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::writer(
        "icebergCompatV1",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableIcebergCompatV1"),
    ),
    DeltaProtocolFeature::writer(
        "icebergCompatV2",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableIcebergCompatV2"),
    ),
    DeltaProtocolFeature::writer(
        "clustering",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::writer(
        "materializePartitionColumns",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "catalogManaged",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "catalogOwned-preview",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "columnMapping",
        DeltaProtocolFeatureClass::NativeOnly,
        2,
        5,
        DeltaProtocolFeatureEnablement::ColumnMappingMode,
    ),
    DeltaProtocolFeature::reader_writer(
        "deletionVectors",
        DeltaProtocolFeatureClass::NativeOnly,
        3,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableDeletionVectors"),
    ),
    DeltaProtocolFeature::reader_writer(
        "timestampNtz",
        DeltaProtocolFeatureClass::NativeOnly,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "typeWidening",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableTypeWidening"),
    ),
    DeltaProtocolFeature::reader_writer(
        "typeWidening-preview",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::BoolFlag("delta.enableTypeWidening"),
    ),
    DeltaProtocolFeature::reader_writer(
        "v2Checkpoint",
        DeltaProtocolFeatureClass::SupportedInBrowser,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "vacuumProtocolCheck",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "variantType",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "variantType-preview",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
    DeltaProtocolFeature::reader_writer(
        "variantShredding-preview",
        DeltaProtocolFeatureClass::TerminalUnsupported,
        3,
        7,
        DeltaProtocolFeatureEnablement::AlwaysIfSupported,
    ),
];

pub fn delta_protocol_feature(feature_name: &str) -> Option<&'static DeltaProtocolFeature> {
    KNOWN_DELTA_PROTOCOL_FEATURES
        .iter()
        .find(|feature| feature.name == feature_name)
}

pub fn delta_protocol_feature_names() -> impl Iterator<Item = &'static str> {
    KNOWN_DELTA_PROTOCOL_FEATURES
        .iter()
        .map(|feature| feature.name)
}

impl DeltaProtocolFeature {
    const fn writer(
        name: &'static str,
        class: DeltaProtocolFeatureClass,
        min_writer_version: i32,
        enablement: DeltaProtocolFeatureEnablement,
    ) -> Self {
        Self {
            name,
            class,
            kind: DeltaProtocolFeatureKind::Writer,
            min_reader_version: 1,
            min_writer_version,
            enablement,
        }
    }

    const fn reader_writer(
        name: &'static str,
        class: DeltaProtocolFeatureClass,
        min_reader_version: i32,
        min_writer_version: i32,
        enablement: DeltaProtocolFeatureEnablement,
    ) -> Self {
        Self {
            name,
            class,
            kind: DeltaProtocolFeatureKind::ReaderWriter,
            min_reader_version,
            min_writer_version,
            enablement,
        }
    }
}
