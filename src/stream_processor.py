import time, argparse
from pathlib import Path
from processor_file import run
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--watch-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--poll-seconds", type=int, default=5)
    args = ap.parse_args()
    last=set()
    while True:
        files=set(str(p) for p in Path(args.watch_dir).glob("*.csv"))
        if files!=last:
            run(Path(args.watch_dir), Path(args.out_dir)); last=files
        time.sleep(args.poll_seconds)
if __name__ == "__main__": main()
