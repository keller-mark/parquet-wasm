use std::sync::Arc;

use crate::error::Result;
use crate::read_options::JsReaderOptions;
use arrow_schema::{DataType, FieldRef};
use arrow_wasm::{Schema, Table};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::arrow::{parquet_to_arrow_field_levels, ProjectionMask};
use parquet::file::metadata::{RowGroupMetaData, RowGroupMetaDataBuilder, ColumnChunkMetaData, ColumnChunkMetaDataBuilder};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, RowGroups, RowSelection};
use parquet::column::page::{PageIterator, PageReader};
use parquet::file::reader::{ChunkReader, Length};
use parquet::file::serialized_reader::SerializedPageReader;
use bytes::{Buf, Bytes};

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
pub fn read_parquet(parquet_file: Vec<u8>, options: JsReaderOptions) -> Result<Table> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();

    let metadata = ArrowReaderMetadata::load(&cursor, Default::default())?;
    let metadata = cast_metadata_view_types(&metadata)?;

    let mut builder = ParquetRecordBatchReaderBuilder::new_with_metadata(cursor, metadata);

    let schema = builder.schema().clone();

    if let Some(batch_size) = options.batch_size {
        builder = builder.with_batch_size(batch_size);
    }

    if let Some(row_groups) = options.row_groups {
        builder = builder.with_row_groups(row_groups);
    }

    if let Some(limit) = options.limit {
        builder = builder.with_limit(limit);
    }

    if let Some(offset) = options.offset {
        builder = builder.with_offset(offset);
    }

    // Create Arrow reader
    let reader = builder.build()?;

    let mut batches = vec![];

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    Ok(Table::new(schema, batches))
}


// Reference: https://github.com/apache/arrow-rs/blob/cbf8045e2f74398196a1408bc4ebd0c4d23afc66/parquet/examples/read_with_rowgroup.rs#L101

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<parquet::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = parquet::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

/// An in-memory column chunk
#[derive(Clone)]
pub struct ColumnChunkData {
    offset: usize,
    data: Bytes,
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> parquet::errors::Result<Bytes> {
        let start = start as usize - self.offset;
        Ok(self.data.slice(start..))
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        self.data.len() as u64
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    // TODO: Try modifying ChunkReader to always adjust its start by the offset.
    // Then, we may not need to adjust anything anywhere else?

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

#[derive(Clone)]
pub struct InMemoryRowGroup {
    pub metadata: RowGroupMetaData,
    column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
}

impl RowGroups for InMemoryRowGroup {
    fn num_rows(&self) -> usize {
        self.metadata.num_rows() as usize
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(parquet::errors::ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(i),
                    self.num_rows(),
                    // TODO: need to override the SerializedPageReader implementation to adjust for byte offsets in pages.

                    None,
                )?);

                Ok(Box::new(ColumnChunkIterator {
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

impl InMemoryRowGroup {
    pub fn new(metadata: RowGroupMetaData, mask: ProjectionMask, row_group_bytes: Vec<u8>) -> Self {
        let mut column_chunks: Vec<Option<Arc<ColumnChunkData>>> = metadata.columns().iter().map(|_| None).collect::<Vec<_>>();

        crate::log!("Row group bytes length: {}", row_group_bytes.len());
        let row_group_offset = metadata.file_offset().unwrap() as u64;
        crate::log!("Row group offset: {}", row_group_offset);

        // Create a fresh RowGroupMetaData to ensure no incorrect byte offsets are included in the row group metadata, or its column metadata.
        // We need to adjust the offsets.
        let mut row_group_builder = metadata.into_builder();
        row_group_builder = row_group_builder.set_file_offset(0);
        row_group_builder = row_group_builder.set_total_byte_size(row_group_bytes.len() as i64);
        for column in row_group_builder.take_columns() {
            let orig_data_page_offset = column.data_page_offset();
            let orig_index_page_offset = column.index_page_offset();
            let orig_dictionary_page_offset = column.dictionary_page_offset();
            let orig_column_index_offset = column.column_index_offset();
            let orig_offset_index_offset = column.offset_index_offset();
            let orig_bloom_filter_offset = column.bloom_filter_offset();

            let column_name = column.column_descr().name();
            let column_type = column.column_descr().self_type().get_basic_info().name();

            if let Some(dict_offset) = orig_dictionary_page_offset {
                let data_offset = orig_data_page_offset;
                crate::log!("Column {}: dictionary_offset={}, data_offset={}, type={}", column_name, dict_offset, data_offset, column_type);
                
                // Check if dictionary is before the row group start
                if dict_offset < row_group_offset as i64 {
                    crate::log!("WARNING: Dictionary page is before row group start!");
                }
            } else {
                crate::log!("Column {}: no dictionary, type={}", column_name, column_type);
            }

            let adjusted_data_page_offset = orig_data_page_offset - row_group_offset as i64;
            let adjusted_index_page_offset = orig_index_page_offset.map(|o| o - row_group_offset as i64);
            let adjusted_dictionary_page_offset = orig_dictionary_page_offset.map(|o| o - row_group_offset as i64);
            let adjusted_column_index_offset = orig_column_index_offset.map(|o| o - row_group_offset as i64);
            let adjusted_offset_index_offset = orig_offset_index_offset.map(|o| o - row_group_offset as i64);
            let adjusted_bloom_filter_offset = orig_bloom_filter_offset.map(|o| o - row_group_offset as i64);
            
            let column = column.into_builder()
                .set_data_page_offset(adjusted_data_page_offset)
                .set_index_page_offset(adjusted_index_page_offset)
                .set_dictionary_page_offset(adjusted_dictionary_page_offset)
                .set_column_index_offset(adjusted_column_index_offset)
                .set_offset_index_offset(adjusted_offset_index_offset)
                .set_bloom_filter_offset(adjusted_bloom_filter_offset)
                .build()
                .unwrap();
            row_group_builder = row_group_builder.add_column_metadata(column);
        }
        let new_metadata = row_group_builder
            .build()
            .unwrap();

        for (leaf_idx, meta) in new_metadata.columns().iter().enumerate() {
            if mask.leaf_included(leaf_idx) {
                let (start, len) = meta.byte_range();
                crate::log!("Column {}: start {}, len {}", leaf_idx, start, len);

                //let data = reader.get_bytes(start..(start + len)).await?;
                // Do we need to use start/offset here, since row_group_bytes is already sliced to the row group?
                // Or, are we slicing into the column chunk data. Do we need to subtract the row group start offset?
                let data = Bytes::copy_from_slice(&row_group_bytes[start as usize..(start + len) as usize]);

                column_chunks[leaf_idx] = Some(Arc::new(ColumnChunkData {
                    offset: start as usize,
                    data,
                }));
            }
        }

        

        Self {
            metadata: new_metadata,
            column_chunks,
        }
    }

}

/// Internal function to read a buffer with Parquet data into a buffer with Arrow IPC Stream data
pub fn read_parquet_row_group(footer_bytes: Vec<u8>, row_group_bytes: Vec<u8>, row_group_index: usize, options: JsReaderOptions) -> Result<Table> {
    // Create Parquet reader
    let m_cursor: Bytes = footer_bytes.clone().into();
    let s_cursor: Bytes = footer_bytes.into();
    let rg_cursor: Bytes = row_group_bytes.into();

    let metadata = ArrowReaderMetadata::load(&m_cursor, Default::default())?;
    let metadata = cast_metadata_view_types(&metadata)?;

    let schema_builder = ParquetRecordBatchReaderBuilder::try_new(s_cursor)?;
    let schema = schema_builder.schema().clone();

    let row_group_metadata: RowGroupMetaData = metadata.metadata().row_group(row_group_index).clone().into();
    let mask = ProjectionMask::all(); // TODO: allow user to specify columns to read

    // Create Arrow reader for single row group
    let levels = parquet_to_arrow_field_levels(
        &row_group_metadata.schema_descr_ptr(),
        mask.clone(),
        None,
    )?;

    let row_groups = InMemoryRowGroup::new(row_group_metadata, mask.clone(), rg_cursor.to_vec());
    let batch_size = 1024; // TODO: allow user to specify batch size
    let selection = None; // TODO: allow user to specify row selection

    let reader = ParquetRecordBatchReader::try_new_with_row_groups(&levels, &row_groups, batch_size, selection)?;

    let mut batches = vec![];

    for maybe_chunk in reader {
        batches.push(maybe_chunk?)
    }

    // Create a new schema to ensure no incorrect byte offsets are included. TODO: is this necessary?
    let new_schema = arrow_schema::SchemaRef::new(arrow_schema::Schema::new(
        schema.fields().clone(),
    ));
    
    let table = Table::new(new_schema, batches);

    Ok(table)
}

/// Internal function to read a buffer with Parquet data into an Arrow schema
pub fn read_schema(parquet_file: Vec<u8>) -> Result<Schema> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new(cursor)?;
    let schema = builder.schema().clone();
    Ok(schema.into())
}

/// Internal function to read a buffer with Parquet data into an Arrow schema
pub fn read_metadata(parquet_file: Vec<u8>) -> Result<ParquetMetaData> {
    // Create Parquet reader
    let cursor: Bytes = parquet_file.into();
    let reader = ParquetMetaDataReader::new();
    let metadata = reader.parse_and_finish(&cursor)?;
    Ok(metadata)
}

/// Cast any view types in the metadata's schema to non-view types
pub(crate) fn cast_metadata_view_types(
    metadata: &ArrowReaderMetadata,
) -> Result<ArrowReaderMetadata> {
    let original_arrow_schema = metadata.schema();
    if has_view_types(original_arrow_schema.fields().iter()) {
        let new_schema = cast_view_types(original_arrow_schema);
        let arrow_options = ArrowReaderOptions::default().with_schema(new_schema);
        Ok(ArrowReaderMetadata::try_new(
            metadata.metadata().clone(),
            arrow_options,
        )?)
    } else {
        Ok(metadata.clone())
    }
}

/// Cast any view types in the schema to non-view types
///
/// Casts:
///
/// - StringView to String
/// - BinaryView to Binary
///
/// Arrow JS does not currently support view types
/// https://github.com/apache/arrow-js/issues/44
fn cast_view_types(schema: &arrow_schema::Schema) -> arrow_schema::SchemaRef {
    let new_fields = _cast_view_types_of_fields(schema.fields().iter());
    Arc::new(arrow_schema::Schema::new_with_metadata(
        new_fields,
        schema.metadata().clone(),
    ))
}

/// Recursively cast any view types in the fields to non-view types
///
/// This includes any view types that are the children of nested types like Structs and Lists
fn _cast_view_types_of_fields<'a>(fields: impl Iterator<Item = &'a FieldRef>) -> Vec<FieldRef> {
    fields
        .map(|field| {
            let new_data_type = match field.data_type() {
                DataType::Utf8View => DataType::Utf8,
                DataType::BinaryView => DataType::Binary,
                DataType::Struct(struct_fields) => {
                    DataType::Struct(_cast_view_types_of_fields(struct_fields.iter()).into())
                }
                DataType::List(inner_field) => DataType::List(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                ),
                DataType::LargeList(inner_field) => DataType::LargeList(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                ),
                DataType::FixedSizeList(inner_field, list_size) => DataType::FixedSizeList(
                    _cast_view_types_of_fields([inner_field].into_iter())
                        .into_iter()
                        .next()
                        .unwrap(),
                    *list_size,
                ),
                other => other.clone(),
            };
            Arc::new(field.as_ref().clone().with_data_type(new_data_type))
        })
        .collect()
}

fn has_view_types<'a>(mut fields: impl Iterator<Item = &'a FieldRef>) -> bool {
    fields.any(|field| match field.data_type() {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(struct_fields) => has_view_types(struct_fields.iter()),
        DataType::List(inner_field) => has_view_types([inner_field].into_iter()),
        DataType::LargeList(inner_field) => has_view_types([inner_field].into_iter()),
        DataType::FixedSizeList(inner_field, _list_size) => {
            has_view_types([inner_field].into_iter())
        }
        _other => false,
    })
}
