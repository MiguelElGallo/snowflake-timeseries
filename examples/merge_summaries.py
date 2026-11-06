"""Combine summary statistics without averaging standard deviations.

Run: python examples/merge_summaries.py
The count n counts valid numeric values, skipping NULLs. Keep a separate event
counter if invalid/missing readings also need to be counted. Inputs are numeric
or None; this is not a general error-quarantine parser.
Uses Decimal and a stable centered second moment (M2), not sum-of-squares minus
sum-squared. A producer must carry n, mean and M2 for each nonoverlapping group.
"""
from dataclasses import dataclass
from decimal import Decimal, localcontext


@dataclass(frozen=True)
class Summary:
    n: int
    mean: Decimal
    m2: Decimal

    @property
    def population_sd(self):
        return (self.m2 / self.n).sqrt() if self.n else None

    @property
    def sample_sd(self):
        return (self.m2 / (self.n - 1)).sqrt() if self.n > 1 else None


def summarize(values):
    values = [Decimal(str(v)) for v in values if v is not None]
    n = len(values)
    mean = sum(values) / n if n else Decimal(0)
    return Summary(n, mean, sum(((v-mean)**2 for v in values), Decimal(0)))


def merge(a, b):
    if not a.n:
        return b
    if not b.n:
        return a
    n = a.n + b.n
    delta = b.mean - a.mean
    return Summary(n, a.mean + delta * b.n / n,
                   a.m2 + b.m2 + delta**2 * a.n * b.n / n)


if __name__ == '__main__':
    with localcontext() as ctx:
        ctx.prec = 50
        left, right = [10, 20, 30], [40]
        combined = merge(summarize(left), summarize(right))
        expected = summarize(left+right)
        assert combined == expected
        assert merge(summarize([]), combined) == combined
        assert summarize([None]).population_sd is None
        assert summarize([7]).sample_sd is None
        print('Count:', combined.n)
        print('Mean:', combined.mean)
        print('Population SD:', combined.population_sd)
        print('Sample SD:', combined.sample_sd)
