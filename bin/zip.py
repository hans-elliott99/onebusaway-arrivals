

import sys
import zipfile
from pathlib import Path

def print_usage():
    print("Usage: ./zip.py srcfile zipfile")

args = sys.argv
if len(args) != 3:
    print_usage()
    exit(1)

srcf = args[1]
zipf = args[2]

if not Path(srcf).exists():
    print_usage()
    print("  srcfile does not exist")
    exit(1)

print(f"Zipping file {srcf} to {zipf}...")
with zipfile.ZipFile(zipf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.write(srcf)

