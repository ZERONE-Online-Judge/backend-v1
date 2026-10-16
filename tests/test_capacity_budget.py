import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    'capacity_budget', Path(__file__).parents[1] / 'deploy' / 'capacity_budget.py')
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def deployment(workers=8, api_pool=4, background_pool=2, overflow=0):
    services = {}
    for name in ('api-blue', 'api-green', *capacity.WORKERS, 'migrate'):
        api = name.startswith('api-')
        services[name] = {
            'environment': {'DATABASE_POOL_SIZE': str(api_pool if api else background_pool),
                'DATABASE_MAX_OVERFLOW': str(overflow)},
            'command': ['uvicorn', 'app.main:app', '--workers', str(workers)] if api else ['python'],
        }
    return {'services': services}


def test_budget_reserves_both_api_colors_and_background_connections():
    result = capacity.validate(deployment(), 100)
    assert result['services']['api-blue'] == 40
    assert result['services']['api-green'] == 40
    assert result['total'] == 90


def test_previous_four_worker_profile_fits_the_same_database():
    assert capacity.validate(deployment(4, 6, 6), 100)['total'] == 86


def test_fourteen_worker_profile_fits_the_expanded_database():
    assert capacity.validate(deployment(14, 2, 2), 128)['total'] == 94


def test_database_expansion_is_required_before_the_new_profile():
    with pytest.raises(ValueError, match='exceeds'):
        capacity.validate(deployment(14, 2, 2), 100)


def test_session_stream_profile_retains_deployment_headroom():
    assert capacity.validate(deployment(14, 3, 2), 160)['total'] == 122
    with pytest.raises(ValueError, match='exceeds'):
        capacity.validate(deployment(14, 3, 2), 128)


def test_worker_increase_is_rejected_before_database_exhaustion():
    with pytest.raises(ValueError, match='rebalance'):
        capacity.validate(deployment(14), 100)


def test_pool_overflow_counts_toward_the_deployment_budget():
    with pytest.raises(ValueError, match='exceeds'):
        capacity.validate(deployment(overflow=1), 100)


@pytest.mark.parametrize('size,overflow', [(0, 0), (-1, 0), (4, -1)])
def test_unbounded_pools_cannot_pass_preflight(size, overflow):
    with pytest.raises(ValueError, match='unbounded'):
        capacity.validate(deployment(api_pool=size, overflow=overflow), 100)


def test_explicit_worker_count_is_required_for_a_reliable_budget():
    config = deployment()
    config['services']['api-blue']['command'] = 'uvicorn app.main:app'
    with pytest.raises(ValueError, match='explicit'):
        capacity.validate(config, 100)


def test_operator_connections_cannot_be_removed_from_the_budget():
    with pytest.raises(ValueError, match='spare'):
        capacity.validate(deployment(), 100, reserve=0)


def test_invalid_environment_values_are_not_printed():
    config = deployment()
    config['services']['api-blue']['environment']['DATABASE_POOL_SIZE'] = 'private-value'
    with pytest.raises(ValueError) as error:
        capacity.validate(config, 100)
    assert 'private-value' not in str(error.value)
    assert 'must be an integer' in str(error.value)
