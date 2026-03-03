"""UK Crime extractor CLI entrypoint.

Exposes `extract.utils.stream_to_s3` via Fire so the DAG or CLI can stream a source URL into S3.
"""

from extract.utils import stream_to_s3
from fire import Fire

if __name__ =="__main__":
    Fire(stream_to_s3)
