"""Check rendered Compose connection bounds without displaying environment values."""
import argparse
import json
import sys

WORKERS = ('mail-worker', 'notice-worker', 'bundle-worker', 'verification-ai-worker')


def _number(value, label):
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f'{label} must be an integer') from None


def connection_budget(config):
    services = config['services']
    result = {}
    for name in ('api-blue', 'api-green', *WORKERS, 'migrate'):
        service = services.get(name)
        if service is None:
            continue
        environment = service.get('environment', {})
        size = _number(environment.get('DATABASE_POOL_SIZE', 6), f'{name} pool size')
        overflow = _number(environment.get('DATABASE_MAX_OVERFLOW', 0), f'{name} pool overflow')
        if size <= 0 or overflow < 0:
            raise ValueError(f'{name}: an unbounded database pool is not allowed')
        processes = 1
        extra = 0
        if name.startswith('api-'):
            argv = service.get('command', [])
            if not isinstance(argv, list) or '--workers' not in argv:
                raise ValueError(f'{name}: explicit API worker count is required')
            processes = _number(argv[argv.index('--workers') + 1], f'{name} worker count')
            if processes <= 0:
                raise ValueError(f'{name}: API worker count must be positive')
            extra = 1  # Dedicated result-wait authentication pool per API process.
        result[name] = processes * (size + overflow + extra)
    return {'services': result, 'total': sum(result.values())}


def validate(config, max_connections, reserve=10):
    if max_connections <= reserve or reserve < 3:
        raise ValueError('PostgreSQL must retain at least three spare connections')
    budget = connection_budget(config)
    if budget['total'] > max_connections - reserve:
        raise ValueError(
            f"Configured connection budget {budget['total']} exceeds "
            f"PostgreSQL capacity {max_connections} minus reserve {reserve}; "
            'rebalance API/worker pools before deployment'
        )
    return {**budget, 'max_connections': max_connections, 'reserve': reserve}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--max-connections', required=True, type=int)
    parser.add_argument('--reserve', default=10, type=int)
    args = parser.parse_args()
    try:
        result = validate(json.load(sys.stdin), args.max_connections, args.reserve)
    except (ValueError, KeyError, IndexError) as error:
        raise SystemExit(f'Database capacity check failed: {error}') from None
    print('Database capacity check: ' + json.dumps(result, sort_keys=True))
