use polars::prelude::*;
use std::path::PathBuf;

use crate::dataset_util::schemas;

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
    LazyCsvReader::new(path)
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
    LazyFrame::scan_parquet(path, args).unwrap()
}

pub(crate) fn write_parquet(df: &mut DataFrame, path: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut file)
        .with_row_group_size(Some(50000))
        .finish(df)
        .unwrap();
}

pub(crate) fn write_parquet_streaming(df: LazyFrame, path: &str) {
    let path = Arc::new(PathBuf::from(path));
    let options = ParquetWriteOptions::default();
    let _ = df.sink_parquet(SinkTarget::Path(path), options, None, SinkOptions::default()).unwrap();
}
