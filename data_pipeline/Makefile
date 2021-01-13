.PHONY: test

default: cov_html 

test:
	PYTHONPATH=. pytest tests/scraper/

cov_html:
	PYTHONPATH=. pytest --cov-report html tests/ --cov

cov_xml:
	PYTHONPATH=. pytest --cov-report xml tests/ --cov

test_all:
	PYTHONPATH=. pytest tests/
