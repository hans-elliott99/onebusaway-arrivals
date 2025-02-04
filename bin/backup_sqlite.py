
import sqlite3
from datetime import datetime

DB_FILE = "./app/data/data.db"

def progress(status, remaining, total):
    print(f"Copied {total-remaining} of {total} pages...")


if __name__ == "__main__":
    backup_file = f"./data/backups/data_{datetime.now().date()}.db"

    con = sqlite3.connect(DB_FILE, check_same_thread=False)
    bck = sqlite3.connect(backup_file, check_same_thread=False)
    with bck:
        con.backup(bck, pages=1, progress=progress)
    bck.close()
    con.close()

