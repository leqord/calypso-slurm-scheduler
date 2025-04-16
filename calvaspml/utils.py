import hashlib
from pathlib import Path


def file_checksum(file_path: Path, algorithm="md5") -> str:
    hash_func = hashlib.new(algorithm)

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_func.update(chunk)

    return hash_func.hexdigest()