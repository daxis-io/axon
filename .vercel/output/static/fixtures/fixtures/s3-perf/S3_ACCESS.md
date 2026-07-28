# S3 Performance Fixture Access

Table URI:

```text
s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table
```

Pinned provenance:

```bash
npm run verify:s3-perf-fixture
```

That validates the committed manifest plus SHA-256 inventory for all 13 Delta
log/checkpoint objects and all 8 active Parquet files. To materialize the pinned
fixture locally from anonymous S3 and verify every object:

```bash
bash scripts/verify-s3-perf-fixture.sh --stage ../../target/fixtures/s3-perf-pinned/table
```

To generate a replacement fixture with the same seeded data shape without
overwriting the pinned metadata:

```bash
npm run build:s3-perf-fixture
```

Review the generated manifest, provenance, and checksum inventory under
`../../target/fixtures/s3-perf-generated` before any explicit promotion or
upload. The generator does not require credentials and skips upload unless the
bucket, prefix, region, and AWS session are all present.

Apply public-read policy and CORS:

```bash
aws s3api put-bucket-policy --bucket axon-public-s3-fixture-452456948477 --policy file://s3-public-read-policy.json
aws s3api put-bucket-cors --bucket axon-public-s3-fixture-452456948477 --cors-configuration file://s3-cors.json
```

Anonymous browser validation:

```bash
curl -sS -D /tmp/axon-s3-perf-list.headers -o /tmp/axon-s3-perf-list.xml \
  -H 'Origin: http://127.0.0.1:5173' \
  'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/?list-type=2&prefix=fixtures/s3-browser-perf/table/_delta_log/&max-keys=5'

curl -sS -D /tmp/axon-s3-perf-log.headers -o /tmp/axon-s3-perf-log.json \
  -H 'Origin: http://127.0.0.1:5173' \
  'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/fixtures/s3-browser-perf/table/_delta_log/00000000000000000000.json'

curl -sS -D /tmp/axon-s3-perf-range.headers -o /tmp/axon-s3-perf-range.bin \
  -H 'Origin: http://127.0.0.1:5173' \
  -H 'Range: bytes=0-3' \
  'https://axon-public-s3-fixture-452456948477.s3.us-east-2.amazonaws.com/fixtures/s3-browser-perf/table/<active-parquet-relative-path>'
```

Browser smoke:

```bash
AXON_LIVE_PUBLIC_S3_TABLE_URI=s3://axon-public-s3-fixture-452456948477/fixtures/s3-browser-perf/table \
AXON_LIVE_PUBLIC_S3_REGION=us-east-2 \
CI=1 npm run test:browser:public-s3-live -- --reporter=line
```
