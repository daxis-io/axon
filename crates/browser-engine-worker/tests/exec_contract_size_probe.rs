#![cfg(feature = "exec-contract-size-probe")]

use browser_engine_worker::exec_contract_size_probe_encoded_len;

#[test]
fn exec_contract_size_probe_encodes_a_representative_request() {
    assert!(exec_contract_size_probe_encoded_len() > 0);
}
