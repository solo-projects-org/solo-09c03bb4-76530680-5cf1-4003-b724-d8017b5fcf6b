#!/usr/bin/env python3
"""Generate the demo sample files into samples/.

  normal_r1/r2.fq.gz        3 matched pairs, 150 bp            (happy path)
  mismatch_r1/r2.fq.gz      pair #2 read name differs          (must be rejected)
  corrupt_r1.fq.gz          truncated gzip member              (must be rejected)
  multimember_r1.fq.gz      two concatenated gzip members      (must be rejected)
  oversize_r1/r2.fq.gz      expands to ~75 MiB > 64 MiB quota  (must be rejected)

The oversize files use highly repetitive content so the .gz stays tiny while
the *expanded* FASTQ blows the default 64 MiB file-byte quota.
"""
import gzip
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent


def write_gz(path: Path, records):
    """records: iterable of (name, seq, qual). Single gzip member."""
    with open(path, "wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz:
            for name, seq, qual in records:
                gz.write(f"@{name}\n{seq}\n+\n{qual}\n".encode())


def rand_seq(n, rng):
    return "".join(rng.choice("ACGTN") for _ in range(n))


def main():
    rng = random.Random(20260825)

    # ---- normal pair: 3 reads x 150 bp, old-style /1 /2 name suffixes ----
    r1, r2 = [], []
    for i in range(3):
        base = f"INST1:5:FLOWCELL1:101:1101:{1000 + i * 37}:16"
        r1.append((f"{base}/1", rand_seq(150, rng), "I" * 150))
        r2.append((f"{base}/2", rand_seq(150, rng), "I" * 150))
    write_gz(HERE / "normal_r1.fq.gz", r1)
    write_gz(HERE / "normal_r2.fq.gz", r2)

    # ---- mismatched pair: read #2 has a different name in R2 ----
    bad_r2 = list(r2)
    name, seq, qual = bad_r2[1]
    bad_r2[1] = (name.replace("1101:1037", "1101:9999"), seq, qual)
    write_gz(HERE / "mismatch_r1.fq.gz", r1)
    write_gz(HERE / "mismatch_r2.fq.gz", bad_r2)

    # ---- corrupt: truncated gzip stream ----
    data = (HERE / "normal_r1.fq.gz").read_bytes()
    (HERE / "corrupt_r1.fq.gz").write_bytes(data[: len(data) * 3 // 5])

    # ---- multi-member gzip (concatenated members are not accepted) ----
    (HERE / "multimember_r1.fq.gz").write_bytes(data + data)

    # ---- oversize: expands past the default 64 MiB quota ----
    # 250 bp reads, ~535 bytes/record expanded; 140_000 reads -> ~75 MiB.
    def oversize_records(suffix):
        for i in range(140_000):
            yield (f"INST1:5:FLOWCELL1:101:1101:{1000 + i}:16/{suffix}",
                   "A" * 250, "I" * 250)

    write_gz(HERE / "oversize_r1.fq.gz", oversize_records(1))
    write_gz(HERE / "oversize_r2.fq.gz", oversize_records(2))

    for p in sorted(HERE.glob("*.fq.gz")):
        print(f"{p.name:26s} {p.stat().st_size:>10,} bytes")


if __name__ == "__main__":
    main()
