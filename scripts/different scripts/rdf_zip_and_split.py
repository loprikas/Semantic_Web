import gzip

# input
INPUT_FILE = "imdb_transformed.ttl"

# output base-name
OUTPUT_PREFIX = "imdb_shard_"

# Shard-Größe in Zeilen
LINES_PER_SHARD = 5_000_000  # (~0.5–2 GB per File)

def shard_ttl(input_file, output_prefix, lines_per_shard):
    shard_index = 1
    line_count = 0
    out_f = gzip.open(f"{output_prefix}{shard_index:04d}.nt.gz", "wt", encoding="utf-8")

    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            # Turtle-Prefix skip
            if line.startswith("@prefix") or not line.strip():
                continue

            out_f.write(line)
            line_count += 1

            if line_count >= lines_per_shard:
                out_f.close()
                print(f"Shard {shard_index:04d} fertig ({line_count} Zeilen)")
                shard_index += 1
                line_count = 0
                out_f = gzip.open(f"{output_prefix}{shard_index:04d}.nt.gz", "wt", encoding="utf-8")

    out_f.close()
    print(f"Shard {shard_index:04d} fertig ({line_count} Zeilen, letzter Shard)")

if __name__ == "__main__":
    shard_ttl(INPUT_FILE, OUTPUT_PREFIX, LINES_PER_SHARD)
