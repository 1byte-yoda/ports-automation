# Ports Automation Case

<!-- PROJECT SHIELDS -->

[![Build Status][build-status-shield]][build-status-url]
[![codecov][code-cov-shield]][code-cov-url]
[![codestyle][code-style]][code-style-url]

<br />

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
      <li><a href="#built-with">Built With</a></li>
        <li>
            <a href="#architecture">Architecture</a>
            <ul><a href="#automation-flow">Automation Flow</a></ul>
        </li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#case-scenarios">Case Scenarios</a></li>
  </ol>
</details>

</br>

## About The Project

This project aims to create an automated data pipeline that can scrape data from a given source and persist the data into a database and a json file. Although designed specifically for [ports][ports-data-url] data, the architecture itself was designed to be extensible to adopt to other use cases and technogly stack.


</br>

## Built With
* [Docker](https://www.docker.com/)
* [Python 3](https://python.org)
* [Airflow](https://airflow.apache.org)
* [Scrapy](https://scrapy.org)
* [PostgresSQL](https://postgresql.org)
* [MongoDB](https://www.mongodb.com/)

</br>

## Architecture
[![Pipeline Architecture][architecture-screenshot]](https://github.com/PHMark/ports-automation/images/architecture.png)


## Automation Flow
* Step 1
* Step 2
* Step N


<!-- MARKDOWN LINKS & IMAGES -->
[architecture-screenshot]: docs/images/architecture.png
[build-status-shield]: https://travis-ci.com/PHMark/ports-automation.svg?branch=main
[build-status-url]: https://travis-ci.com/PHMark/ports-automation
[code-cov-shield]: https://codecov.io/gh/PHMark/ports-automation/branch/main/graph/badge.svg?token=ZQ23COSI3V
[code-cov-url]: https://codecov.io/gh/PHMark/ports-automation
[code-style]: https://img.shields.io/badge/codestyle-flake8-28df99
[code-style-url]: https://github.com/PHMark/ports-automation
[ports-data-url]: https://unece.org/cefact/unlocode-code-list-country-and-territory
