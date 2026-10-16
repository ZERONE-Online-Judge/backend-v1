"""Hold synthetic session-event streams; never log credentials or event bodies."""
import argparse
import asyncio
from collections import Counter
import json
from pathlib import Path
import time

import httpx


async def main(args):
    groups = {}
    for actor in json.loads(Path(args.actors).read_text()):
        groups.setdefault(actor['contest_id'], []).append(actor)
    actors = [actor for batch in zip(*groups.values()) for actor in batch][:args.users]
    assert len(actors) == args.users
    counts = Counter()
    started = time.monotonic()
    deadline = started + args.seconds
    active = peak = 0
    connected_at = None
    limits = httpx.Limits(max_connections=args.users + 5,
        max_keepalive_connections=args.users + 5)
    async with httpx.AsyncClient(base_url=args.url, limits=limits,
        timeout=httpx.Timeout(5, read=None), trust_env=False) as client:
        health = (await client.get('/api/health')).json()
        assert health['data']['env'] == 'capacity', 'Never load the public API'

        async def hold(index, actor):
            nonlocal active, peak, connected_at
            await asyncio.sleep(index / args.users * 5)
            try:
                async with client.stream('GET', '/api/auth/session-events',
                    headers={'Authorization': 'Bearer ' + actor['token']}) as response:
                    counts[str(response.status_code)] += 1
                    if response.status_code != 200:
                        return
                    active += 1
                    peak = max(peak, active)
                    if active == args.users and connected_at is None:
                        connected_at = round(time.monotonic()-started, 2)
                        if args.ready_file:
                            Path(args.ready_file).write_text(json.dumps({'users':args.users,
                                'connected_after_seconds':connected_at,'at':time.time()}))
                    try:
                        iterator = response.aiter_bytes()
                        while time.monotonic() < deadline:
                            try:
                                data = await asyncio.wait_for(anext(iterator),
                                    timeout=max(0.01, deadline - time.monotonic()))
                            except TimeoutError:
                                return
                            except StopAsyncIteration:
                                counts['ended_early'] += 1
                                return
                            counts['bytes'] += len(data)
                    finally:
                        active -= 1
            except httpx.HTTPError as error:
                counts[type(error).__name__] += 1
        await asyncio.gather(*(hold(i, actor) for i, actor in enumerate(actors)))
    Path(args.output).write_text(json.dumps({'users':args.users,
        'elapsed_seconds':round(time.monotonic()-started, 2),
        'peak_open_streams':peak, 'counts':dict(counts),
        'all_connected_after_seconds':connected_at}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', default='http://127.0.0.1:18081')
    parser.add_argument('--actors', default='/capacity/actors.json')
    parser.add_argument('--users', type=int, default=1000)
    parser.add_argument('--seconds', type=int, default=60)
    parser.add_argument('--output', required=True)
    parser.add_argument('--ready-file')
    asyncio.run(main(parser.parse_args()))
