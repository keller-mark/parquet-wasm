import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq

compressions = ["SNAPPY", "GZIP", "BROTLI", "LZ4", "ZSTD", "NONE"]


def create_data():
    data = {
        "str": pa.array(["a", "b", "c", "d"], type=pa.string()),
        "uint8": pa.array([1, 2, 3, 4], type=pa.uint8()),
        "int32": pa.array([0, -2147483638, 2147483637, 1], type=pa.int32()),
        "bool": pa.array([True, True, False, False], type=pa.bool_()),
    }
    return pa.table(data)


def write_data(table):
    feather.write_feather(table, "data.arrow", compression="uncompressed")

    data_len = len(table)

    for n_partitions in [1, 2]:
        for compression in compressions:
            row_group_size = data_len / n_partitions
            compression_text = str(compression).lower()
            fname = f"{n_partitions}-partition-{compression_text}.parquet"
            pq.write_table(
                table, fname, row_group_size=row_group_size, compression=compression
            )

def write_data2():
    data = {
        "str": pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i"], type=pa.string()),
        "uint8": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9], type=pa.uint8()),
        "int32": pa.array([0, -2147483638, 2147483637, 1, 11, 2, 12, 13, 14], type=pa.int32()),
        "bool": pa.array([True, True, False, False, True, False, True, False, True], type=pa.bool_()),
    }
    table = pa.table(data)

    for n_partitions in [1, 2]:
        for compression in compressions:
            row_group_size = 2
            compression_text = str(compression).lower()
            fname = f"{n_partitions}-partition-{compression_text}-multiple-row-groups.parquet"
            pq.write_table(
                table, fname, row_group_size=row_group_size, compression=compression
            )

def write_data3():
    data = {
        "str": pa.array(["a", "b", "c", "d", "e", "f", "g", "h", "i"], type=pa.string()),
        "uint8": pa.array([1, 2, 3, 4, 5, 6, 7, 8, 9], type=pa.uint8()),
        "int32": pa.array([0, -2147483638, 2147483637, 1, 11, 2, 12, 13, 14], type=pa.int32()),
        "bool": pa.array([True, True, False, False, True, False, True, False, True], type=pa.bool_()),
        "dict": pa.array(["a", "b", "a", "c", "a", "b", "a", "c", "a"], type=pa.dictionary(pa.int8(), pa.string())),
    }
    table = pa.table(data)

    for n_partitions in [1, 2]:
        for compression in compressions:
            row_group_size = 2
            compression_text = str(compression).lower()
            fname = f"{n_partitions}-partition-{compression_text}-multiple-row-groups-and-dict.parquet"
            pq.write_table(
                table, fname, row_group_size=row_group_size, compression=compression
            )


def write_empty_table():
    pd.DataFrame().to_parquet("empty.parquet")


def create_string_view_table():
    data = {
        "string_view": pa.array(["a", "b", "c", "d"], type=pa.string_view()),
        "binary_view": pa.array([b"a", b"b", b"c", b"d"], type=pa.binary_view()),
    }
    return pa.table(data)


def write_string_view_table():
    table = create_string_view_table()
    pq.write_table(table, "string_view.parquet", compression="snappy")


def main():
    table = create_data()
    write_data(table)
    write_empty_table()
    write_string_view_table()
    write_data2()
    write_data3()


if __name__ == "__main__":
    main()
