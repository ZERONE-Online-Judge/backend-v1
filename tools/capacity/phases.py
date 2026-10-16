"""Run bounded k6 scenarios against the disposable capacity API only."""
import argparse
import json
import re
from pathlib import Path
import subprocess
import threading
import time
import urllib.request

IMAGE = "grafana/k6@sha256:e66db15b860113878fa74670e31f5e274830b7b6e42c8bff28b2f2d86a257603"
PHASES = [
    ("browser-1000-150rps", {"USERS":"1000","RATE":"150","DURATION":"120s","PROFILE":"interactive","WRITES":"true"}),
    ("browser-1000-200rps", {"USERS":"1000","RATE":"200","DURATION":"60s","PROFILE":"interactive","WRITES":"true"}),
    ("steady-1000-200rps", {"USERS":"1000","RATE":"200","DURATION":"60s","PROFILE":"mixed","WRITES":"true"}),
    ("burst-3000-500rps", {"USERS":"3000","RATE":"500","DURATION":"20s","PROFILE":"mixed","WRITES":"true"}),
    ("burst-3000-1000rps", {"USERS":"3000","RATE":"1000","DURATION":"20s","PROFILE":"mixed","WRITES":"true"}),
    ("continuous-1000", {"USERS":"1000","DURATION":"20s","PROFILE":"mixed","WRITES":"true"}),
    ("waiting-1000", {"USERS":"1000","DURATION":"20s","PROFILE":"status"}),
]


def main(args):
    folder = Path(args.folder)
    assert (folder/'capacity.env').exists() and (folder/'actors.json').exists()
    inspected = json.loads(subprocess.check_output(['docker','inspect','zoj-capacity-api']))[0]
    assert 'APP_ENV=capacity' in inspected['Config']['Env'], "Not a disposable capacity API"
    with urllib.request.urlopen('http://127.0.0.1:18081/api/health', timeout=5) as response:
        target = json.load(response)['data']
        assert target['env'] == 'capacity', 'Never load the public API'
    assert not args.label or re.fullmatch(r'[a-z0-9-]+', args.label)
    manifest = []
    for phase, env in PHASES:
        if args.only and phase not in args.only:
            continue
        name = (args.label+'-' if args.label else '')+phase
        stop = threading.Event()
        failures = []
        def guard():
            misses = 0
            while not stop.wait(2):
                available = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                    if line.startswith('MemAvailable:')))
                try:
                    with urllib.request.urlopen('http://127.0.0.1:6001/api/health', timeout=3) as response:
                        misses = 0 if response.status == 200 else misses+1
                except Exception:
                    misses += 1
                if misses >= 3 or available < 2*1024*1024:
                    failures.append('production-health' if misses >= 3 else 'available-memory')
                    subprocess.run(['docker','stop','-t','1','zoj-capacity-load'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                    return
        watcher = threading.Thread(target=guard, daemon=True)
        watcher.start()
        command = ['docker','run','--rm','--name','zoj-capacity-load','--network','host','--user','0',
            '--ulimit','nofile=16384:16384','-v',str(folder)+':/capacity']
        for key, value in {**env,'OUTPUT':'/capacity/k6-'+name+'.json'}.items():
            command += ['-e',key+'='+value]
        command += [IMAGE,'run','--quiet','/capacity/workload.js']
        started = time.time()
        try:
            with (folder/('k6-'+name+'.log')).open('w') as log:
                result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
        finally:
            stop.set()
            watcher.join(timeout=5)
        path = folder/('k6-'+name+'.json')
        metrics = json.loads(path.read_text())['metrics'] if path.exists() else {}
        values = lambda key: metrics.get(key,{}).get('values',{})
        record = {'phase':name, 'target':target, 'parameters':env,'started_at':started,'elapsed_seconds':round(time.time()-started,2),
            'exit_code':result.returncode, 'guard_stop':failures,
            'requests':values('http_reqs'),'unexpected':values('unexpected_error'),
            'rejected':values('rejected'),'dropped_iterations':values('dropped_iterations'),
            'submit_ms':values('success_submit'),'scoreboard_ms':values('success_scoreboard'),
            'submissions_ms':values('success_submissions')}
        manifest.append(record)
        print(json.dumps(record),flush=True)
        (folder/((args.label+'-' if args.label else '')+'phase-manifest.json')).write_text(json.dumps(manifest,indent=2))
        if failures:
            break


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder',default='/home/zoj/.cache/api-capacity-20261016')
    parser.add_argument('--only',nargs='*')
    parser.add_argument('--label',default='')
    main(parser.parse_args())
