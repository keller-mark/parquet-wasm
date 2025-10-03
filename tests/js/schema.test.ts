import * as wasm from "../../pkg/node/parquet_wasm";
import { readFileSync } from "fs";
import * as arrow from "apache-arrow";
import { tableFromIPC as altTableFromIPC } from '@uwdata/flechette';
import { readExpectedArrowData, extractFooterBytes, extractRowGroupBytes } from "./utils";
import { parseSchema } from "arrow-js-ffi";
import { it, expect } from "vitest";

// Path from repo root
const dataDir = "tests/data";

const WASM_MEMORY = wasm.wasmMemory();

it("read schema via FFI", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ffiSchema = wasm.readSchema(arr).intoFFI();

  const schema = parseSchema(WASM_MEMORY.buffer, ffiSchema.addr());

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length
  );
});

it("read schema via IPC", async (t) => {
  const expectedTable = readExpectedArrowData();

  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const ipcSchema = wasm.readSchema(arr).intoIPCStream();

  const schema = arrow.tableFromIPC(ipcSchema).schema;

  expect(expectedTable.schema.fields.length).toStrictEqual(
    schema.fields.length
  );
});

it("read metadata from full file bytes", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  // TODO: test with footer bytes alone as well
  const metadata = wasm.readMetadata(arr);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  // Test against the existing ParquetFile.metadata method.
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(expectedMetadata.fileMetadata().createdBy());
  expect(metadata.fileMetadata().numRows()).toStrictEqual(expectedMetadata.fileMetadata().numRows());
  expect(metadata.fileMetadata().version()).toStrictEqual(expectedMetadata.fileMetadata().version());
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(expectedMetadata.numRowGroups());
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(expectedMetadata.rowGroup(0).numRows());
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(289);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(269);
});

it("read metadata from footer bytes only", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  // Test against the existing ParquetFile.metadata method.
  const expectedMetadata = pqFile.metadata();

  expect(metadata.fileMetadata().createdBy()).toStrictEqual(expectedMetadata.fileMetadata().createdBy());
  expect(metadata.fileMetadata().numRows()).toStrictEqual(expectedMetadata.fileMetadata().numRows());
  expect(metadata.fileMetadata().version()).toStrictEqual(expectedMetadata.fileMetadata().version());
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.numRowGroups()).toStrictEqual(expectedMetadata.numRowGroups());
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(expectedMetadata.rowGroup(0).numRows());
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(289);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(269);
});

it("read single row group from bytes", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  
  expect(metadata.numRowGroups()).toStrictEqual(1);
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(4);
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(289);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(269);

  const rowGroupBytes = extractRowGroupBytes(arr, metadata, 0);
  expect(rowGroupBytes.length).toStrictEqual(289);

  const rowGroupTable = arrow.tableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes, 0).intoIPCStream());

  expect(rowGroupTable.schema.fields.length).toStrictEqual(4);
  expect(rowGroupTable.numRows).toStrictEqual(4);

  const rows = rowGroupTable.toArray().map(r => r.toJSON());
  expect(rows).toEqual([
    {"str": "a", "uint8": 1, "int32": 0, "bool": true},
    {"str": "b", "uint8": 2, "int32": -2147483638, "bool": true},
    {"str": "c", "uint8": 3, "int32": 2147483637, "bool": false},
    {"str": "d", "uint8": 4, "int32": 1, "bool": false}
  ]);
});

it("read single row group from bytes, more than one row group, small table", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli-multiple-row-groups.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  
  expect(metadata.numRowGroups()).toStrictEqual(5);
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(2);
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(267);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(240);

  const rowGroupBytes = extractRowGroupBytes(arr, metadata, 0);
  expect(rowGroupBytes.length).toStrictEqual(267);

  const rowGroupTable = arrow.tableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes, 0).intoIPCStream());

  expect(rowGroupTable.schema.fields.length).toStrictEqual(4);
  expect(rowGroupTable.numRows).toStrictEqual(2);

  const rows = rowGroupTable.toArray().map(r => r.toJSON());
  expect(rows).toEqual([
    {"str": "a", "uint8": 1, "int32": 0, "bool": true},
    {"str": "b", "uint8": 2, "int32": -2147483638, "bool": true}
  ]);

  // Read a second row group.

  const rowGroupBytes2 = extractRowGroupBytes(arr, metadata, 1);
  expect(rowGroupBytes2.length).toStrictEqual(268);

  const rowGroupTable2 = arrow.tableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes2, 1).intoIPCStream());

  expect(rowGroupTable2.schema.fields.length).toStrictEqual(4);
  expect(rowGroupTable2.numRows).toStrictEqual(2);

  const rows2 = rowGroupTable2.toArray().map(r => r.toJSON());
  expect(rows2).toEqual([
    {"str": "c", "uint8": 3, "int32": 2147483637, "bool": false},
    {"str": "d", "uint8": 4, "int32": 1, "bool": false}
  ]);

});

it("read single row group from bytes, more than one row group, small table with dictionary encoded column", async (t) => {
  const dataPath = `${dataDir}/1-partition-brotli-multiple-row-groups-and-dict.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);
  
  expect(metadata.numRowGroups()).toStrictEqual(5);
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(2);
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(339);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(306);

  const rowGroupBytes = extractRowGroupBytes(arr, metadata, 0);
  expect(rowGroupBytes.length).toStrictEqual(339);

  const rowGroupTable = altTableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes, 0).intoIPCStream());

  expect(rowGroupTable.schema.fields.length).toStrictEqual(5);
  expect(rowGroupTable.numRows).toStrictEqual(2);

  const rows = rowGroupTable.toArray().map(r => r.toJSON());
  expect(rows).toEqual([
    {"str": "a", "uint8": 1, "int32": 0, "bool": true, "dict": "a"},
    {"str": "b", "uint8": 2, "int32": -2147483638, "bool": true, "dict": "b"}
  ]);

  // Read a second row group.
  const rowGroupBytes2 = extractRowGroupBytes(arr, metadata, 1);
  expect(rowGroupBytes2.length).toStrictEqual(268);

  const rowGroupTable2 = altTableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes2, 1).intoIPCStream());

  expect(rowGroupTable2.schema.fields.length).toStrictEqual(5);
  expect(rowGroupTable2.numRows).toStrictEqual(2);

  const rows2 = rowGroupTable2.toArray().map(r => r.toJSON());
  expect(rows2).toEqual([
    {"str": "c", "uint8": 3, "int32": 2147483637, "bool": false, "dict": "a"},
    {"str": "d", "uint8": 4, "int32": 1, "bool": false, "dict": "c"}
  ]);

});

it.skip("read single row group from bytes, from table with more than one row group, big table", async (t) => {
  const dataPath = `${dataDir}/part.0.parquet`;
  const buffer = readFileSync(dataPath);
  const arr = new Uint8Array(buffer);
  const footerBytes = extractFooterBytes(arr);
  const metadata = wasm.readMetadata(footerBytes);

  const ipcSchema = wasm.readSchema(arr).intoIPCStream();
  const schema = arrow.tableFromIPC(ipcSchema).schema;

  const expectedTable = wasm.readParquet(arr).intoIPCStream();
  const expectedArrowTable = arrow.tableFromIPC(expectedTable);

  const firstRow = expectedArrowTable.toArray()[0];
  console.log("First row of full table:", firstRow.toJSON());
  

  // Convert the parquet file buffer from readFileSync to a Blob.
  const blob = new Blob([buffer], { type: "application/octet-stream" });
  const pqFile = await wasm.ParquetFile.fromFile(blob);

  expect(arr.length).toStrictEqual(199394457);
  
  // Test with first row group
  expect(metadata.numRowGroups()).toStrictEqual(92);
  expect(metadata.rowGroup(0).numRows()).toStrictEqual(50000);
  expect(metadata.rowGroup(0).fileOffset()).toStrictEqual(4);
  expect(metadata.rowGroup(0).compressedSize()).toStrictEqual(2187392);
  expect(metadata.rowGroup(0).totalByteSize()).toStrictEqual(2576209);

  const rowGroupBytes = extractRowGroupBytes(arr, metadata, 0);
  expect(rowGroupBytes.length).toStrictEqual(2187392);

  const rowGroupTable = arrow.tableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes, 0).intoIPCStream());

  expect(rowGroupTable.schema.fields.length).toStrictEqual(12);
  expect(rowGroupTable.numRows).toStrictEqual(50000);

  console.log(rowGroupTable.toArray()[0]);

  // Expect the schema from the row group to match the schema from the full file
  for (let i = 0; i < schema.fields.length; i++) {
    expect(rowGroupTable.schema.fields[i].name).toStrictEqual(schema.fields[i].name);
    expect(rowGroupTable.schema.fields[i].type.toString()).toStrictEqual(schema.fields[i].type.toString());
  }

  console.log(rowGroupTable.schema.fields.map(f => f.name));
  console.log(rowGroupTable.schema.fields.map(f => f.type.toString()));

  // Ground truth value of first row's transcript_id and cell_id
  expect(rowGroupTable.getChild("transcript_id").get(0).toString()).toStrictEqual("281474976710656");
  expect(rowGroupTable.getChild("cell_id").get(0).toString()).toStrictEqual("565");


  // TODO: test with second row group as well
  // Test with second row group
  expect(metadata.rowGroup(1).numRows()).toStrictEqual(50000);
  expect(metadata.rowGroup(1).fileOffset()).toStrictEqual(4369284);
  expect(metadata.rowGroup(1).compressedSize()).toStrictEqual(2174942);
  expect(metadata.rowGroup(1).totalByteSize()).toStrictEqual(2569913);

  const rowGroupBytes2 = extractRowGroupBytes(arr, metadata, 1);
  expect(rowGroupBytes2.length).toStrictEqual(2174942);

  const rowGroupTable2 = arrow.tableFromIPC(wasm.readParquetRowGroup(footerBytes, rowGroupBytes2, 1).intoIPCStream());

  expect(rowGroupTable2.schema.fields.length).toStrictEqual(12);
  expect(rowGroupTable2.numRows).toStrictEqual(50000);

  // Print column names
  console.log(rowGroupTable.schema.fields.map(f => f.name));

  console.log(rowGroupTable2.toArray()[0])

  expect(rowGroupTable2.getChild("transcript_id").get(0).toString()).toStrictEqual("281474976710656");

});

