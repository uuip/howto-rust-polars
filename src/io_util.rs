use crate::dataset_util::schemas;
use polars::prelude::sync_on_close::SyncOnCloseType;
use polars::prelude::*;

pub(crate) fn read_csv(path: &str) -> DataFrame {
    let schema = SchemaRef::from(schemas());
    CsvReadOptions::default()
        .with_has_header(false)
        .with_schema(Some(schema))
        .try_into_reader_with_file_path(Some(path.into()))
        .unwrap()
        .finish()
        .unwrap()
}

pub(crate) fn read_csv_lazy(path: &str) -> LazyFrame {
    LazyCsvReader::new(PlRefPath::new(path))
        .with_has_header(false)
        .with_schema(Some(SchemaRef::from(schemas())))
        .finish()
        .unwrap()
}

pub(crate) fn read_parquet(path: &str) -> DataFrame {
    let mut file = std::fs::File::open(path).unwrap();
    ParquetReader::new(&mut file).finish().unwrap()
}

pub(crate) fn read_parquet_lazy(path: &str) -> LazyFrame {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(PlRefPath::new(path), args).unwrap()
}

pub(crate) fn write_parquet(df: &mut DataFrame, path: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut file)
        .with_row_group_size(Some(50000))
        .finish(df)
        .unwrap();
}

pub(crate) fn write_parquet_streaming(df: LazyFrame, path: &str) {
    let path = PlRefPath::new(path);
    let options = ParquetWriteOptions::default();
    let unified_sink_args = UnifiedSinkArgs {
        mkdir: false,
        maintain_order: true,
        sync_on_close: SyncOnCloseType::None,
        cloud_options: None,
    };
    let _ = df
        .sink(
            SinkDestination::File {
                target: SinkTarget::Path(path),
            },
            FileWriteFormat::Parquet(Arc::new(options)),
            unified_sink_args,
        )
        .unwrap();
}
