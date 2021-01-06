import unittest
import click


@click.group()
def cli():
    """
    CLI to manage unittests and airflow commands.
    """
    pass


@cli.command('tests')
def run_tests():
    """Runs the tests without code coverage"""
    test_loader = unittest.TestLoader()
    tests_dir = "plugins/helpers/scraper/unece_ports/tests"
    tests = test_loader.discover(tests_dir, pattern="test*.py")
    result = unittest.TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        return 0
    return 1


if __name__ == '__main__':
    cli()
