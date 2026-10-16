"""Rewrite a Parquet file with nanosecond timestamps as microseconds, split into parts.

The archive files store times as TIMESTAMP(NANOS), which Databricks refuses to
read (PARQUET_TYPE_ILLEGAL). Microseconds are plenty for train times.

Output is split into parts of about 2 million rows (roughly 80 MB). One 600 MB
upload kept failing with "broken pipe"; small parts upload reliably and a retry
only repeats one part.

    python scripts/convert_timestamps.py in.parquet out_dir/
"""

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def micro_schema(schema):
    fields = []
    for field in schema:
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            field = field.with_type(pa.timestamp("us", tz=field.type.tz))
        fields.append(field)
    return pa.schema(fields, metadata=schema.metadata)


def convert(src, out_dir, rows_per_part=2_000_000, batch_size=500_000):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    source = pq.ParquetFile(src)
    target_schema = micro_schema(source.schema_arrow)

    rows, part, writer, rows_in_part = 0, 0, None, 0
    try:
        for batch in source.iter_batches(batch_size=batch_size):
            if writer is None or rows_in_part >= rows_per_part:
                if writer is not None:
                    writer.close()
                writer = pq.ParquetWriter(out_dir / f"part-{part:03d}.parquet", target_schema,
                                          compression="zstd")
                part += 1
                rows_in_part = 0
            # safe=False: drop sub-microsecond digits instead of raising
            table = pa.Table.from_batches([batch]).cast(target_schema, safe=False)
            writer.write_table(table)
            rows += table.num_rows
            rows_in_part += table.num_rows
    finally:
        if writer is not None:
            writer.close()
    return rows, part


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: convert_timestamps.py SRC OUT_DIR")
    total, parts = convert(sys.argv[1], sys.argv[2])
    print(f"{total} rows written in {parts} parts")
