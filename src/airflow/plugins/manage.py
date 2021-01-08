# plugins/manage_scrapers.py

from os.path import join
import click
import coverage
from unittest import TestLoader, TextTestRunner


# Scrapy's spider base directory
SCRAPER_BASE_DIR = 'plugins/helpers/scraper/unece_ports/'
PLUGINS_BASE_DIR = 'plugins'


COV = coverage.coverage(
    branch=True,
    include=[
        join(SCRAPER_BASE_DIR, '*'),
        join(PLUGINS_BASE_DIR, 'helpers/lib/*')
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


@cli.command('test')
def run_scraper_tests():
    """Runs the tests without code coverage"""
    test_loader = TestLoader()
    scraper_tests_dir = join(SCRAPER_BASE_DIR, 'tests')
    scraper_tests = test_loader.discover(scraper_tests_dir, pattern="test*.py")
    scraper_result = TextTestRunner(verbosity=2).run(scraper_tests)

    test_loader = TestLoader()
    plugins_tests_dir = join(PLUGINS_BASE_DIR, 'tests')
    plugins_tests = test_loader.discover(plugins_tests_dir, pattern="test*.py")
    plugins_result = TextTestRunner(verbosity=2).run(plugins_tests)
    if scraper_result.wasSuccessful() and plugins_result.wasSuccessful():
        return 0
    return 1


@cli.command('cov')
def cov():
    """Runs the unit tests with coverage."""
    test_loader = TestLoader()
    scraper_tests_dir = join(SCRAPER_BASE_DIR, 'tests')
    scraper_tests = test_loader.discover(
        scraper_tests_dir,
        pattern="test*.py"
    )
    scraper_result = TextTestRunner(verbosity=2).run(scraper_tests)

    test_loader = TestLoader()
    plugins_tests_dir = join(PLUGINS_BASE_DIR, 'tests')
    plugins_tests = test_loader.discover(
        plugins_tests_dir,
        pattern="test*.py"
    )
    plugins_result = TextTestRunner(verbosity=2).run(plugins_tests)
    result = scraper_result.wasSuccessful() and plugins_result.wasSuccessful()
    if result:
        COV.stop()
        COV.save()
        print('Coverage Summary:')
        COV.report()
        COV.html_report()
        COV.xml_report()
        return 0
    import sys
    sys.exit(result)


if __name__ == '__main__':
    cli()
