"""Partitioned ETL: a producer writes partitions, a consumer merges them.

The consumer does not spell out every partition path; it declares a
``glob_dep`` pattern.  Partitions can be added/removed between runs and the
consumer is re-run exactly when the matched set or the content changes.
"""

import os


def task_generate():
    """produce this run's partitions"""
    def generate():
        os.makedirs('landing/2022-04', exist_ok=True)
        with open('landing/2022-03.csv', 'w') as handle:
            handle.write('march\n')
        with open('landing/2022-04/april.csv', 'w') as handle:
            handle.write('april\n')
        return True
    return {'actions': [generate],
            'targets': ['landing/2022-03.csv', 'landing/2022-04/april.csv']}


def task_merge():
    """merge every partition that exists right now"""
    def merge(dependencies):
        with open('merged.csv', 'w') as handle:
            handle.write(''.join(open(path).read() for path in dependencies))
        return True
    return {'actions': [merge],
            'glob_dep': ['landing/**/*.csv'],
            'targets': ['merged.csv']}
