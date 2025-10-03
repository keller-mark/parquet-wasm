use parquet::column::page::{PageIterator, PageReader, Page, PageMetadata};
use parquet::file::reader::{ChunkReader};
use parquet::file::serialized_reader::{SerializedPageReader, SerializedPageReaderState};
use parquet::errors::{ParquetError, Result};
use parquet::format::{PageHeader, PageLocation, PageType};
use parquet::file::{
    metadata::*,
    properties::{ReaderProperties, ReaderPropertiesPtr},
    reader::*,
    statistics,
};

fn verify_page_header_len(header_len: usize, remaining_bytes: u64) -> Result<()> {
    if header_len as u64 > remaining_bytes {
        return Err(eof_err!("Invalid page header"));
    }
    Ok(())
}

fn verify_page_size(
    compressed_size: i32,
    uncompressed_size: i32,
    remaining_bytes: u64,
) -> Result<()> {
    // The page's compressed size should not exceed the remaining bytes that are
    // available to read. The page's uncompressed size is the expected size
    // after decompression, which can never be negative.
    if compressed_size < 0 || compressed_size as u64 > remaining_bytes || uncompressed_size < 0 {
        return Err(eof_err!("Invalid page header"));
    }
    Ok(())
}

impl<R: ChunkReader> PageReader for SerializedPageReader<R> {
    fn get_next_page(&mut self) -> Result<Option<Page>> {
        loop {
            let page = match &mut self.state {
                SerializedPageReaderState::Values {
                    offset,
                    remaining_bytes: remaining,
                    next_page_header,
                    page_index,
                    require_dictionary,
                } => {
                    if *remaining == 0 {
                        return Ok(None);
                    }

                    let mut read = self.reader.get_read(*offset)?;
                    let header = if let Some(header) = next_page_header.take() {
                        *header
                    } else {
                        let (header_len, header) = Self::read_page_header_len(
                            &self.context,
                            &mut read,
                            *page_index,
                            *require_dictionary,
                        )?;
                        verify_page_header_len(header_len, *remaining)?;
                        *offset += header_len as u64;
                        *remaining -= header_len as u64;
                        header
                    };
                    verify_page_size(
                        header.compressed_page_size,
                        header.uncompressed_page_size,
                        *remaining,
                    )?;
                    let data_len = header.compressed_page_size as usize;
                    *offset += data_len as u64;
                    *remaining -= data_len as u64;

                    if header.type_ == PageType::INDEX_PAGE {
                        continue;
                    }

                    let mut buffer = Vec::with_capacity(data_len);
                    let read = read.take(data_len as u64).read_to_end(&mut buffer)?;

                    if read != data_len {
                        return Err(eof_err!(
                            "Expected to read {} bytes of page, read only {}",
                            data_len,
                            read
                        ));
                    }

                    let buffer =
                        self.context
                            .decrypt_page_data(buffer, *page_index, *require_dictionary)?;

                    let page = decode_page(
                        header,
                        Bytes::from(buffer),
                        self.physical_type,
                        self.decompressor.as_mut(),
                    )?;
                    if page.is_data_page() {
                        *page_index += 1;
                    } else if page.is_dictionary_page() {
                        *require_dictionary = false;
                    }
                    page
                }
                SerializedPageReaderState::Pages {
                    page_locations,
                    dictionary_page,
                    page_index,
                    ..
                } => {
                    let (front, is_dictionary_page) = match dictionary_page.take() {
                        Some(front) => (front, true),
                        None => match page_locations.pop_front() {
                            Some(front) => (front, false),
                            None => return Ok(None),
                        },
                    };

                    let page_len = usize::try_from(front.compressed_page_size)?;
                    let buffer = self.reader.get_bytes(front.offset as u64, page_len)?;

                    let (offset, header) = Self::read_page_header_len_from_bytes(
                        &self.context,
                        buffer.as_ref(),
                        *page_index,
                        is_dictionary_page,
                    )?;
                    let bytes = buffer.slice(offset..);
                    let bytes =
                        self.context
                            .decrypt_page_data(bytes, *page_index, is_dictionary_page)?;

                    if !is_dictionary_page {
                        *page_index += 1;
                    }
                    decode_page(
                        header,
                        bytes,
                        self.physical_type,
                        self.decompressor.as_mut(),
                    )?
                }
            };

            return Ok(Some(page));
        }
    }

    fn peek_next_page(&mut self) -> Result<Option<PageMetadata>> {
        match &mut self.state {
            SerializedPageReaderState::Values {
                offset,
                remaining_bytes,
                next_page_header,
                page_index,
                require_dictionary,
            } => {
                loop {
                    if *remaining_bytes == 0 {
                        return Ok(None);
                    }
                    return if let Some(header) = next_page_header.as_ref() {
                        if let Ok(page_meta) = (&**header).try_into() {
                            Ok(Some(page_meta))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            *next_page_header = None;
                            continue;
                        }
                    } else {
                        let mut read = self.reader.get_read(*offset)?;
                        let (header_len, header) = Self::read_page_header_len(
                            &self.context,
                            &mut read,
                            *page_index,
                            *require_dictionary,
                        )?;
                        verify_page_header_len(header_len, *remaining_bytes)?;
                        *offset += header_len as u64;
                        *remaining_bytes -= header_len as u64;
                        let page_meta = if let Ok(page_meta) = (&header).try_into() {
                            Ok(Some(page_meta))
                        } else {
                            // For unknown page type (e.g., INDEX_PAGE), skip and read next.
                            continue;
                        };
                        *next_page_header = Some(Box::new(header));
                        page_meta
                    };
                }
            }
            SerializedPageReaderState::Pages {
                page_locations,
                dictionary_page,
                total_rows,
                page_index: _,
            } => {
                if dictionary_page.is_some() {
                    Ok(Some(PageMetadata {
                        num_rows: None,
                        num_levels: None,
                        is_dict: true,
                    }))
                } else if let Some(page) = page_locations.front() {
                    let next_rows = page_locations
                        .get(1)
                        .map(|x| x.first_row_index as usize)
                        .unwrap_or(*total_rows);

                    Ok(Some(PageMetadata {
                        num_rows: Some(next_rows - page.first_row_index as usize),
                        num_levels: None,
                        is_dict: false,
                    }))
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn skip_next_page(&mut self) -> Result<()> {
        match &mut self.state {
            SerializedPageReaderState::Values {
                offset,
                remaining_bytes,
                next_page_header,
                page_index,
                require_dictionary,
            } => {
                if let Some(buffered_header) = next_page_header.take() {
                    verify_page_size(
                        buffered_header.compressed_page_size,
                        buffered_header.uncompressed_page_size,
                        *remaining_bytes,
                    )?;
                    // The next page header has already been peeked, so just advance the offset
                    *offset += buffered_header.compressed_page_size as u64;
                    *remaining_bytes -= buffered_header.compressed_page_size as u64;
                } else {
                    let mut read = self.reader.get_read(*offset)?;
                    let (header_len, header) = Self::read_page_header_len(
                        &self.context,
                        &mut read,
                        *page_index,
                        *require_dictionary,
                    )?;
                    verify_page_header_len(header_len, *remaining_bytes)?;
                    verify_page_size(
                        header.compressed_page_size,
                        header.uncompressed_page_size,
                        *remaining_bytes,
                    )?;
                    let data_page_size = header.compressed_page_size as u64;
                    *offset += header_len as u64 + data_page_size;
                    *remaining_bytes -= header_len as u64 + data_page_size;
                }
                if *require_dictionary {
                    *require_dictionary = false;
                } else {
                    *page_index += 1;
                }
                Ok(())
            }
            SerializedPageReaderState::Pages {
                page_locations,
                dictionary_page,
                page_index,
                ..
            } => {
                if dictionary_page.is_some() {
                    // If a dictionary page exists, consume it by taking it (sets to None)
                    dictionary_page.take();
                } else {
                    // If no dictionary page exists, simply pop the data page from page_locations
                    if page_locations.pop_front().is_some() {
                        *page_index += 1;
                    }
                }

                Ok(())
            }
        }
    }

    fn at_record_boundary(&mut self) -> Result<bool> {
        match &mut self.state {
            SerializedPageReaderState::Values { .. } => Ok(self.peek_next_page()?.is_none()),
            SerializedPageReaderState::Pages { .. } => Ok(true),
        }
    }
}