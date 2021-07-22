# UNECE Port Data Pipeline

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
        <li><a href="#architecture">Architecture</a></li>
        <li><a href="#automation-flow">Automation Flow</a></li>
        <li><a href="#dag">DAG</a></li>
        </ul>
        <li>
            <a href="#test-coverage-snapshot">Test Coverage Snapshot</a>
        </li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#helpful-resources">Helpful Resources</a></li>
      </ul>
    </li>
  </ol>
</details>

</br>

## About The Project

This project aims to create an automated data pipeline that can scrape data from a given source and persist the data into a database and a json file. Although designed specifically for [ports][ports-data-url] data, the architecture itself was designed to be extensible to adopt to other use cases and technology stack.


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
[![Pipeline Architecture][architecture-screenshot]](https://github.com/PHMark/ports-automation/blob/main/docs/images/architecture.png)

</br>

## Data Flow
* As the first component, we have the data ingestion phase which currently checks the scraper if it can request the right data on the web by running scrapy contracts
* Then if the check passes, it will scrape the data.
* After that, the scraped data, while unstructured and uncleaned, will be ingested/upserted into the staging database.
* After that, the ETL operator will extract the data from the staging db, then perform some processing like cleaning & standardizing columns, and sends it to the master database.
* Master database as the name implies, holds the master data which were cleaned and ready to be used by downstream processes.
* A data validator will then run some checks on the data. Ex. number of rows
* If the validation passed, it will load the data to the sink (JSON or/and *Reporting).
* It will lastly send a notification with regards to the DAG's run status.


</br>

## DAG
[![DAG][dag-screenshot]](https://github.com/PHMark/ports-automation/blob/main/docs/images/dag.png)

</br>

## Test Coverage Snapshot
[![Code Test Coverage][code-cov-screenshot]](https://github.com/PHMark/ports-automation/blob/main/docs/images/cov-test.png)


## Getting Started
### Prerequisites

Make sure you have the following software installed and running on your computer:

* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)

Check docker version
```sh
$ docker -v
Docker version 19.03.13, build 4484c46d9d
```

Check docker-compose version
```sh
$ docker-compose -v
docker-compose version 1.27.4, build 40524192
```

### Installation

1. Clone the repo
```sh
$ git clone https://github.com/1byte-yoda/ports-automation.git
```


2. Make sure you are in the root folder "ports-automation", and edit the environment [ports-automation/data-pipeline/.env.example](https://github.com/1byte-yoda/ports-automation/blob/main/data_pipeline/.env.example) file to update the following variables:

```
# [*******INIT THE FF. VARIABLES********]
# SMTP and Slack setup for notification
SMTP_PASSWORD=16_DIGIT_APP_PASSWORD
SMTP_USER=YOUR_EMAIL_ADDRESS
SMTP_PORT=587
SMTP_HOST=smtp.gmail.com
SMTP_MAIL_FROM=YOUR_EMAIL_ADDRESS
SLACK_API_KEY=57_DIGIT_API_KEY
```
I used gmail as the default SMTP but feel free to modify the configuration. You may also find this <a href="#helpful-resources">resources</a> useful.


3. Install using docker-compose
```sh
$ docker-compose -f docker-compose-dev.yml up --build
```
Wait for airflow to initialize the database, scheduler, and the web server. This might take a few minutes depending on your machine.

4. Visit http://localhost:8080, you just successfully replicate the app on your machine!

### Helpful Resources:
  
  - You can follow this [link](https://api.slack.com/authentication/basics) to create your SLACK_API_KEY
  
  - And the 6 steps listed on this [link](https://stackoverflow.com/questions/51829200/how-to-set-up-airflow-send-email) to create your SMTP_PASSWORD


<!-- MARKDOWN LINKS & IMAGES -->
[architecture-screenshot]: docs/images/architecture.png
[code-cov-screenshot]: docs/images/cov-test.png
[dag-screenshot]: docs/images/dag.png
[build-status-shield]: https://travis-ci.com/1byte-yoda/ports-automation.svg?branch=main
[build-status-url]: https://travis-ci.com/1byte-yoda/ports-automation
[code-cov-shield]: https://codecov.io/gh/1byte-yoda/ports-automation/branch/main/graph/badge.svg?token=ZQ23COSI3V
[code-cov-url]: https://codecov.io/gh/1byte-yoda/ports-automation
[code-style]: https://img.shields.io/badge/codestyle-flake8-28df99
[code-style-url]: https://github.com/1byte-yoda/ports-automation
[ports-data-url]: https://unece.org/cefact/unlocode-code-list-country-and-territory
