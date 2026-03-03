"""Land Registry extractor CLI entrypoint.

This module exposes the shared streaming helper as a Fire CLI command.
"""

from fire import Fire

from extract.utils import stream_to_s3

if __name__ == "__main__":
    Fire(stream_to_s3)
