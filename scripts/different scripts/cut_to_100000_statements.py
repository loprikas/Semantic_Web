import sys, gzip, io, os

def open_any(path, mode):
    textmode = 't' in mode or 'b' not in mode
    if path.endswith('.gz'):
        return gzip.open(path, mode if 't' in mode or 'b' in mode else mode+'t', encoding='utf-8') if textmode else gzip.open(path, mode)
    return open(path, mode, encoding='utf-8') if textmode else open(path, mode)

def main():
    if len(sys.argv) < 3:
        print("Usage: python cut_nt.py <input.nt|input.nt.gz> <output.nt.gz> [max_lines=100000]")
        sys.exit(1)

    inp = sys.argv[1]
    outp = sys.argv[2]
    max_lines = int(sys.argv[3]) if len(sys.argv) > 3 else 100_000

    # Ausgabe immer komprimiert schreiben (.gz), spart Platz
    if not outp.endswith('.gz'):
        print("Hinweis: Ausgabe wird unkomprimiert sein. Für .gz bitte Dateiname mit .gz wählen.")

    count = 0
    with open_any(inp, 'rt') as fin, open_any(outp, 'wt') as fout:
        for line in fin:
            if not line.strip():            # leere Zeilen überspringen
                continue
            fout.write(line)
            count += 1
            if count >= max_lines:
                break

    print(f"Fertig: {count} Tripel -> {outp}")

if __name__ == "__main__":
    main()
