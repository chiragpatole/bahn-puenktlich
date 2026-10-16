from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.convert_timestamps import convert


def test_nanoseconds_become_microseconds_split_into_parts(tmp_path):
    src = tmp_path / "src.parquet"
    times = [datetime(2026, 7, 1, 8, 15), None, datetime(2026, 7, 31, 23, 59), datetime(2026, 7, 2), None]
    table = pa.table({
        "eva": ["8011160", "8012666", None, "8000105", "8002549"],
        "time": pa.array(times, type=pa.timestamp("ns")),
    })
    pq.write_table(table, src)

    rows, parts = convert(src, tmp_path / "out", rows_per_part=2, batch_size=2)

    assert (rows, parts) == (5, 3)
    files = sorted((tmp_path / "out").glob("part-*.parquet"))
    assert [f.name for f in files] == ["part-000.parquet", "part-001.parquet", "part-002.parquet"]
    out = pa.concat_tables(pq.read_table(f) for f in files)
    assert out.schema.field("time").type == pa.timestamp("us")
    assert out.column("time").to_pylist() == times
    assert out.column("eva").to_pylist() == table.column("eva").to_pylist()
