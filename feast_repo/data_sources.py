from feast import FileSource, PushSource

# ✅ BATCH SOURCE
pispi_batch_source = FileSource(
    path="s3://pispi-features-batch/pispi_features.parquet",
    timestamp_field="event_timestamp",
)

# ✅ STREAM SOURCE (le vrai fix)
stream_features_source = PushSource(
    name="stream_features",              # ⚠️ DOIT MATCHER store.push()
    batch_source=pispi_batch_source      # ⚠️ hybrid mode
)