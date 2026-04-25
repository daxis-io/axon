use wasm_delta_snapshot::{BrowserDeltaLogManifest, BrowserDeltaLogObject};

#[test]
fn browser_delta_log_manifest_lists_delta_log_paths_in_sorted_order() {
    let manifest = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000001.json",
                "https://example.com/table/_delta_log/1.json?sig=secret",
            ),
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/table/_delta_log/0.json?sig=secret",
            ),
        ],
    )
    .expect("manifest should validate");

    assert_eq!(
        manifest.list_paths("_delta_log"),
        vec![
            "_delta_log/00000000000000000000.json".to_string(),
            "_delta_log/00000000000000000001.json".to_string(),
        ]
    );
}

#[test]
fn browser_delta_log_manifest_rejects_duplicate_or_escaping_paths() {
    let duplicate = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/a",
            ),
            BrowserDeltaLogObject::new(
                "_delta_log/00000000000000000000.json",
                "https://example.com/b",
            ),
        ],
    )
    .expect_err("duplicate log paths should fail");
    assert!(duplicate.message.contains("duplicate"));

    let escaping = BrowserDeltaLogManifest::new(
        "gs://bucket/table",
        vec![BrowserDeltaLogObject::new(
            "../_delta_log/00000000000000000000.json",
            "https://example.com/a",
        )],
    )
    .expect_err("escaping paths should fail");
    assert!(escaping.message.contains("_delta_log"));
}
