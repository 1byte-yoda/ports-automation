# plugins/manage_scrapers.py

from os.path import join
import click
import coverage
from unittest import TestLoader, TextTestRunner


# Scrapy's spider base directory
BASE_DIR = 'plugins/helpers/scraper/unece_ports/'


COV = coverage.coverage(
    branch=True,
    include=[join(BASE_DIR, '*')],
    omit=[
        join(BASE_DIR, 'tests/*'),
        join(BASE_DIR, 'postgresql.py'),
        join(BASE_DIR, 'pipelines/kafka/*'),
        join(BASE_DIR, '__init__.py')
    ]
)
COV.start()


@click.group()
def cli():
    """
    CLI to manage unittests and airflow commands.
    """
    pass


@cli.command('test_scrapers')
def run_scraper_tests():
    """Runs the tests without code coverage"""
    test_loader = TestLoader()
    tests_dir = join(BASE_DIR, 'tests')
    tests = test_loader.discover(tests_dir, pattern="test*.py")
    result = TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        return 0
    return 1


@cli.command('scraper_cov')
def cov():
    """Runs the unit tests with coverage."""
    test_loader = TestLoader()
    tests = test_loader.discover(join(BASE_DIR, 'tests'))
    result = TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        COV.stop()
        COV.save()
        print('Coverage Summary:')
        COV.report()
        COV.html_report()
        COV.erase()
        return 0
    import sys
    sys.exit(result)


if __name__ == '__main__':
    cli()
