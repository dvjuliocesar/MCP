import time, argparse, hashlib
from pathlib import Path
from .processor_file import run

def snapshot(dirpath: Path):
    sigs = []
    for p in sorted(dirpath.glob("*.csv")):
        try:
            st = p.stat()
            sigs.append(f"{p.name}|{st.st_size}|{int(st.st_mtime)}")
        except FileNotFoundError:
            continue
    return hashlib.md5("\n".join(sigs).encode()).hexdigest(), len(sigs)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--poll-seconds", type=int, default=5)
    args = ap.parse_args()

    watch = Path(args.watch_dir)
    out = Path(args.out_dir)
    poll = args.poll_seconds

    print(f"[watch] observando: {watch.resolve()}  (poll={poll}s)")
    run(watch, out)
    print("[watch] primeira execução concluída.")

    last_sig, last_n = snapshot(watch)
    print(f"[watch] snapshot inicial: {last_n} arquivos .csv")

    try:
        while True:
            time.sleep(poll)
            sig, n = snapshot(watch)
            if sig != last_sig:
                print(f"[watch] mudança detectada ({last_n} -> {n}). Reprocessando…")
                run(watch, out)
                print("[watch] pipeline concluído.")
                last_sig, last_n = sig, n
            else:
                print("[watch] sem mudanças…")
    except KeyboardInterrupt:
        print("\n[watch] encerrado pelo usuário. Até mais 👋")

if __name__ == "__main__":
    main()

