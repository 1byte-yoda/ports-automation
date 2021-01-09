# plugins/manage_scrapers.py


import time
from os.path import join
import click
import coverage
from unittest import TestLoader, TextTestRunner


# Scrapy's spider base directory
SCRAPER_BASE_DIR = 'plugins/helpers/scraper/unece_ports/'

# Airflow plugins base directory
PLUGINS_BASE_DIR = 'plugins'

# Airflow dags base directory
DAGS_BASE_DIR = 'dags'


COV = coverage.coverage(
    branch=True,
    include=[
        join(SCRAPER_BASE_DIR, '*'),
        join(PLUGINS_BASE_DIR, 'helpers/lib/*'),
        join(DAGS_BASE_DIR, '*')
    ],
    omit=[
        join(SCRAPER_BASE_DIR, 'tests/*'),
        join(SCRAPER_BASE_DIR, 'postgresql.py'),
        join(SCRAPER_BASE_DIR, 'pipelines/kafka/*'),
        join(SCRAPER_BASE_DIR, '__init__.py'),
        join(SCRAPER_BASE_DIR, 'pipelines/__init__.py'),
        join(SCRAPER_BASE_DIR, 'spiders/__init__.py'),
        join(SCRAPER_BASE_DIR, 'spiders/lib/__init__.py'),
        join(PLUGINS_BASE_DIR, 'helpers/lib/sql_queries.py'),
        join(PLUGINS_BASE_DIR, 'helpers/lib/__init__.py')
    ]
)
COV.start()


@click.group()
def cli():
    """
    CLI to manage unittests and airflow commands.
    """
    pass


def run_scraper_pipeline_tests():
    """Runs the tests for scraper's pipeline"""
    print("RUNNING SCRAPER's PIPELINE TESTS ....")
    test_loader = TestLoader()
    scraper_tests_dir = join(SCRAPER_BASE_DIR, 'tests')
    scraper_tests = test_loader.discover(scraper_tests_dir, pattern="test*.py")
    total_tests = scraper_tests.countTestCases()
    start_time = time.time()
    scraper_result = TextTestRunner(verbosity=2).run(scraper_tests)
    total_time = time.time() - start_time
    if scraper_result.wasSuccessful():
        return total_tests, total_time, scraper_result.wasSuccessful()
    return [0]*3


@cli.command('test')
def run_tests():
    """Runs the tests without code coverage"""
    scraper_tests, perf_time, scraper_success = run_scraper_pipeline_tests()
    test_loader = TestLoader()
    plugins_tests_dir = join(PLUGINS_BASE_DIR, 'tests')
    plugin_tests = test_loader.discover(plugins_tests_dir, pattern="test*.py")
    start_time = time.time()
    plugins_result = TextTestRunner(verbosity=2).run(plugin_tests)
    total_time = start_time - time.time()
    total_runtime = perf_time - total_time
    total_tests = scraper_tests + plugin_tests.countTestCases()
    print("\n", "*"*70)
    print(f"Ran {total_tests} tests in {round(total_runtime, 3)}s\n")
    if plugins_result.wasSuccessful() and scraper_success:
        print("OK\n")
        return 0
    print("FAILED\n")
    return 1


@cli.command('cov')
def cov():
    """Runs the unit tests with coverage."""
    test_loader = TestLoader()
    plugins_tests_dir = join(PLUGINS_BASE_DIR, 'tests')
    plugins_tests = test_loader.discover(
        plugins_tests_dir,
        pattern="test*.py"
    )
    plugins_result = TextTestRunner(verbosity=2).run(plugins_tests)
    result = plugins_result.wasSuccessful()
    if result:
        COV.stop()
        COV.save()
        print('Coverage Summary:')
        COV.report()
        COV.html_report()
        return 0
    import sys
    sys.exit(result)


if __name__ == '__main__':
    cli()
