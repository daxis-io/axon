use std::io;

pub(crate) fn write_all_at(
    bytes: &[u8],
    offset: usize,
    mut write: impl FnMut(&[u8], usize) -> io::Result<usize>,
) -> io::Result<usize> {
    let mut written = 0usize;
    while written < bytes.len() {
        let at = offset
            .checked_add(written)
            .ok_or_else(|| io::Error::other("spill write offset overflow"))?;
        let remaining = &bytes[written..];
        let next = write(remaining, at)?;
        if next == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "spill_storage/io_failure/write_zero",
            ));
        }
        if next > remaining.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "spill_storage/io_failure/invalid_write_length",
            ));
        }
        written = written
            .checked_add(next)
            .ok_or_else(|| io::Error::other("spill write offset overflow"))?;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Cursor};
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_ipc::reader::StreamReader;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};

    use super::write_all_at;

    #[test]
    fn short_host_writes_preserve_a_complete_arrow_ipc_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![11, 22, 33, 44]))],
        )
        .unwrap();
        let mut ipc = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut ipc, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let mut stored = Vec::new();
        write_all_at(&ipc, 0, |bytes, at| {
            let written = bytes.len().min(7);
            let end = at + written;
            if stored.len() < end {
                stored.resize(end, 0);
            }
            stored[at..end].copy_from_slice(&bytes[..written]);
            Ok(written)
        })
        .unwrap();

        let decoded = StreamReader::try_new(Cursor::new(stored), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let values = decoded
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.values(), &[11, 22, 33, 44]);
    }

    #[test]
    fn zero_length_host_write_is_an_io_failure() {
        let error = write_all_at(&[1, 2, 3], 0, |_bytes, _at| Ok(0)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WriteZero);
        assert_eq!(error.to_string(), "spill_storage/io_failure/write_zero");
    }
}
