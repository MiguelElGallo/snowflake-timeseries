"""Independent exact-membership oracle; no Snowflake dependencies."""
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
import math

METRICS = ('N_EVENTS', 'N_VALID', 'MEAN', 'STD_POP', 'STD_SAMP', 'MIN_VALUE', 'MAX_VALUE')


def timestamp(value):
    return datetime.fromisoformat(value) if isinstance(value, str) else value


def canonical(events):
    result = {}
    for e in events:
        key = (e['run_id'], e['event_id'])
        if key in result:
            previous = result[key]
            assert all(previous[k] == e[k] for k in ('sensor_id', 'event_ts', 'value')), key
        result[key] = e
    return list(result.values())


def number(value):
    try:
        x = Decimal(str(value))
        return x if x.is_finite() else None
    except Exception:
        return None


def stats(events):
    values = [number(e['value']) for e in events]
    values = [v for v in values if v is not None]
    n = len(values)
    if not n:
        return [len(events), 0, None, None, None, None, None]
    with localcontext() as ctx:
        ctx.prec = 50
        mean = sum(values) / n
        m2 = sum((v - mean) ** 2 for v in values)
        pop = (m2 / n).sqrt()
        samp = (m2 / (n - 1)).sqrt() if n > 1 else None
        return [len(events), n, float(mean), float(pop), float(samp) if samp is not None else None,
                float(min(values)), float(max(values))]


def grouped(events, kind='minute', asofs=()):
    groups = defaultdict(list)
    unique = canonical(events)
    if kind == 'minute':
        for e in unique:
            minute = timestamp(e['event_ts']).replace(second=0, microsecond=0)
            groups[(e['run_id'], e['sensor_id'], minute)].append(e)
    elif kind == 'asof':
        for run, end in asofs:
            for e in unique:
                if e['run_id'] == run and end - timedelta(seconds=60) < timestamp(e['event_ts']) <= end:
                    groups[(run, e['sensor_id'], end)].append(e)
    elif kind == 'range':
        for anchor in unique:
            end = timestamp(anchor['event_ts'])
            groups[(anchor['run_id'], anchor['event_id'])] = [e for e in unique
                if e['run_id'] == anchor['run_id'] and e['sensor_id'] == anchor['sensor_id']
                and end - timedelta(seconds=60) <= timestamp(e['event_ts']) <= end]
    else:
        raise ValueError(kind)
    return {key: stats(rows) for key, rows in groups.items()}


def compare(actual, expected):
    """Map keys -> seven metrics; NULL and key coverage are explicit."""
    assert set(actual) == set(expected), {
        'missing': list(set(expected) - set(actual))[:10], 'extra': list(set(actual) - set(expected))[:10]}
    maximum_error = {m: 0.0 for m in METRICS[2:]}
    for key, want in expected.items():
        got = actual[key]
        assert len(got) == len(want) == 7
        assert list(got[:2]) == list(want[:2]), (key, got, want)
        large = want[2] is not None and abs(want[2]) >= 1e10
        for metric, a, b in zip(METRICS[2:], got[2:], want[2:]):
            if a is None or b is None:
                assert a is None and b is None, (key, metric, a, b)
                continue
            a, b = float(a), float(b)
            assert math.isfinite(a) and math.isfinite(b), (key, metric, a, b)
            err = abs(a - b)
            maximum_error[metric] = max(maximum_error[metric], err)
            tolerance = 1e-4 if large else 1e-8
            assert err <= tolerance, (key, metric, a, b, err, tolerance)
    return {'groups': len(expected), 'maximum_absolute_error': maximum_error}


def fixture():
    base = datetime(2000, 1, 1, 12)
    events = []
    def add(name, sensor, sec, value, seq=1):
        e = {'run_id': 'fixture', 'event_id': name, 'sensor_id': sensor,
             'event_ts': (base + timedelta(seconds=sec)).isoformat(), 'value': value,
             'batch_seq': seq, 'sent_at': base.isoformat()}
        events.append(e)
        return e
    for i, sec in enumerate((0, 60, 120, 180)):
        add(f'late_anchor_{i}', 'late', sec, 10 * (i + 1))
    for i, sec in enumerate((-0.000001, 0, 0.000001, 59.999999, 60, 60.000001)):
        add(f'boundary_{i}', 'boundary', sec, i + 1)
    add('equal_1', 'equal', 12, 7)
    add('equal_2', 'equal', 12, 7)
    add('null_1', 'null', 3, None)
    add('null_2', 'null', 4, 'invalid')
    add('single_1', 'single', 6, 13)
    add('single_null', 'single', 7, None)
    for i, v in enumerate((10**12-2, 10**12-1, 10**12, 10**12+1, 10**12+2)):
        add(f'numeric_{i}', 'numeric', i, v)
    initial = list(events)
    late = add('genuinely_late', 'late', 15, 99, 2)
    replay = dict(initial[0], batch_seq=3)
    return base, initial, late, replay


def self_check():
    base, initial, late, replay = fixture()
    assert stats([{'value': 5}]) == [1, 1, 5.0, 0.0, None, 5.0, 5.0]
    assert stats([{'value': None}]) == [1, 0, None, None, None, None, None]
    a = grouped(initial, 'asof', [('fixture', base + timedelta(seconds=60))])
    assert a[('fixture', 'boundary', base + timedelta(seconds=60))][0] == 3
    r = grouped(initial, 'range')
    assert r[('fixture', 'boundary_4')][0] == 4
    before = grouped(initial + [late])
    after = grouped(initial + [late, replay])
    assert before == after
    assert before[('fixture', 'numeric', base)][3] == float(Decimal(2).sqrt())
    assert grouped(initial + [late], 'range')[('fixture', 'late_anchor_1')][0] == 3
    # The oracle itself must reject silent NULLs and missing/extra keys.
    key = next(iter(before))
    bad = dict(before)
    bad[key] = list(bad[key]); bad[key][2] = None
    try:
        compare(bad, before)
    except AssertionError:
        pass
    else:
        raise AssertionError('NULL mismatch accepted')
    return {'status': 'passed', 'fixture_events': len(initial)}


if __name__ == '__main__':
    print(self_check())
