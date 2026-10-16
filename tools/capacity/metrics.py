"""Record aggregate resource/DB counters, never user records or credentials."""
import argparse
import json
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument("--seconds", type=int, default=60)
p.add_argument("--output", required=True)
args = p.parse_args()
deadline = time.monotonic()+args.seconds
with Path(args.output).open("w") as output:
    while time.monotonic() < deadline:
        sql = "SELECT json_build_object('connections',count(*),'active',count(*) FILTER(WHERE state='active'),'lock_waits',count(*) FILTER(WHERE wait_event_type='Lock')) FROM pg_stat_activity; SELECT json_build_object('commits',xact_commit,'rollbacks',xact_rollback,'inserted',tup_inserted,'updated',tup_updated,'returned',tup_returned,'cache_hits',blks_hit,'disk_reads',blks_read) FROM pg_stat_database WHERE datname='zoj_capacity_20261016';"
        result = subprocess.run(["docker","exec","deploy-postgres-1","sh","-c",
            'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "$1"', "sh", sql],
            capture_output=True,text=True,timeout=5)
        sample = {"at":time.time(),"load":Path('/proc/loadavg').read_text().split()[:3],
            "memory":{line.split(':')[0]:line.split(':')[1].strip() for line in Path('/proc/meminfo').read_text().splitlines()
                if line.startswith(('MemAvailable:','SwapFree:'))},
            "db":[json.loads(line) for line in result.stdout.splitlines()] if result.returncode == 0 else []}
        output.write(json.dumps(sample)+'\n'); output.flush()
        time.sleep(2)
