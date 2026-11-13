"""Streaming single-member gzip decompression + 4-line FASTQ validation."""
import itertools
import zlib


class FastqError(Exception):
    """Format/validation error in the uploaded data."""


class Cancelled(Exception):
    """Raised when the batch was cancelled/terminated mid-processing."""


def _check_cancel(should_cancel) -> None:
    if should_cancel():
        raise Cancelled()


def decompress_validate_fastq(src_path, dst_path, quota, should_cancel,
                              chunk_size: int = 262144) -> int:
    """Decompress *src_path* (must be a single-member gzip) to *dst_path*
    while validating strict 4-line FASTQ.

    The shared quota is charged before every write, so the limit is enforced
    while receiving, not after the whole file landed. Returns the read count.
    Raises FastqError, QuotaExceeded, Cancelled, OSError.
    """
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    pending = b""
    trailing_bad = False  # any non-NUL byte after the single gzip member
    line_no = 0          # global line index; line_no % 4 selects the record field
    seq_len = -1
    reads = 0

    def process_line(line: bytes) -> None:
        nonlocal seq_len, reads, line_no
        field = line_no % 4
        if field == 0:
            if not line.startswith(b"@") or len(line) <= 1:
                raise FastqError(f"line {line_no + 1}: expected '@<name>' header")
        elif field == 1:
            if not line:
                raise FastqError(f"line {line_no + 1}: empty sequence")
            seq_len = len(line)
        elif field == 2:
            if not line.startswith(b"+"):
                raise FastqError(f"line {line_no + 1}: expected '+' separator line")
        else:
            if len(line) != seq_len:
                raise FastqError(
                    f"line {line_no + 1}: quality length {len(line)} "
                    f"!= sequence length {seq_len}"
                )
            reads += 1
        line_no += 1

    with open(src_path, "rb") as src, open(dst_path, "wb") as dst:
        while True:
            _check_cancel(should_cancel)
            chunk = src.read(chunk_size)
            if not chunk:
                break
            data = chunk
            while data:
                # Bound each decompress step so quota checks (and memory)
                # stay granular even for highly compressible input.
                try:
                    out = d.decompress(data, chunk_size)
                except zlib.error as e:
                    raise FastqError(f"invalid gzip stream: {e}") from e
                data = d.unconsumed_tail
                if d.unused_data:
                    # The single member ended before the input did: anything
                    # after it is a concatenated member or trailing garbage.
                    if d.unused_data.strip(b"\x00"):
                        trailing_bad = True
                    rest = src.read(chunk_size)
                    while rest:
                        if rest.strip(b"\x00"):
                            trailing_bad = True
                        rest = src.read(chunk_size)
                    data = b""
                if out:
                    quota.acquire(len(out))
                    dst.write(out)
                    pending += out
                    *lines, pending = pending.split(b"\n")
                    for line in lines:
                        process_line(line.rstrip(b"\r"))
            if d.unused_data:
                break
        if not d.eof:
            raise FastqError("truncated gzip member (unexpected end of stream)")
        if trailing_bad:
            raise FastqError(
                "not a single-member gzip (concatenated members or trailing data)"
            )
        if pending:
            process_line(pending.rstrip(b"\r"))
    if line_no % 4 != 0:
        raise FastqError(f"truncated FASTQ: {line_no} lines is not a multiple of 4")
    if reads == 0:
        raise FastqError("empty FASTQ: no reads")
    return reads


def _read_names(path):
    """Yield read names (header token, trailing /1 or /2 stripped) per record."""
    with open(path, "rb") as f:
        i = 0
        for line in f:
            if i % 4 == 0:
                name = line[1:].split()[0] if line.startswith(b"@") else b""
                if name.endswith((b"/1", b"/2")):
                    name = name[:-2]
                yield name
            i += 1


def compare_pairs(r1_path, r2_path, should_cancel) -> int:
    """Stream both expanded FASTQs and require identical read names pair by
    pair (after stripping trailing /1, /2) and equal read counts."""
    count = 0
    sentinel = object()
    for a, b in itertools.zip_longest(
        _read_names(r1_path), _read_names(r2_path), fillvalue=sentinel
    ):
        _check_cancel(should_cancel)
        if a is sentinel or b is sentinel:
            raise FastqError("read count differs between R1 and R2")
        count += 1
        if a != b:
            raise FastqError(
                f"read pair #{count} name mismatch: R1={a[:80]!r} R2={b[:80]!r}"
            )
    return count
