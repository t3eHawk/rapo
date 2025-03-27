# Rapo

[![version](https://img.shields.io/pypi/v/rapo)](https://pypi.org/project/rapo/)
[![release](https://img.shields.io/github/v/release/t3eHawk/rapo?include_prereleases)](https://github.com/t3eHawk/rapo/)
[![release date](https://img.shields.io/github/release-date-pre/t3eHawk/rapo)](https://github.com/t3eHawk/rapo/releases/)
[![last commit](https://img.shields.io/github/last-commit/t3eHawk/rapo)](https://github.com/t3eHawk/rapo/commits/)
[![downloads](https://img.shields.io/pypi/dm/rapo)](https://pypi.org/project/rapo/)
[![python](https://img.shields.io/pypi/pyversions/rapo)](https://pypi.org/project/rapo/)
[![license](https://img.shields.io/pypi/l/rapo)](https://mit-license.org)

## Revenue Assurance Processes Optimizer
Rapo is a Python instrument based on relational databases to build Revenue Assurance controls and, as a result, cover critical business risks and detect revenue leaks.

## Prologue
If you are part of the Revenue Assurance Team, then you probably know that the core is a system of controls that allows you to perform your daily responsibilities and generate reports required by business.

Usually, this system is implemented using third-party software.
Sometimes it is provided by special vendors, and sometimes it is packed with the billing system.

Anyway, the license purchase is required, as is probably the recruitment of the integration team.
This causes an additional investment, which could not be confirmed by the business sponsors.

In addition, such software is usually used in part because many of features are outdated or not required. Outdated design is also often encountered and can be a problem.

So Rapo is created by RA engineers to solve such problems and provides a modern and simple alternative solution for the RA system.

## Advantages
If you are a young RA Team or looking for some alternatives, try Rapo because:
* Free to use right now and here.
* Easy start with low installation efforts.
* Easy control preparation based on SQL, which should not be a problem for RA experts.
* Open-source Python technology, so it won't be an issue to find an expert who can maintain or even improve the solution for your specific needs.
* Built-in Python interface that allows you to integrate the system, including control results, with some popular data science tools or machine learning modules.
* Ready-to-use REST API that allows you to interact with controls or send the control results to some Dashboard or reporting tool.
* This is a developing project with an open feature list and many plans.
* Last but not least, Rapo is created by RA specialists with more than 10 years of expiriens, hundreds of found incidents, and, in turn, millions in saved revenue for their company and investors.

## Installation
Start with install using _pip_:
```
pip install rapo
```

1. Deploy the database schema using the [scripts](schema/oracle.sql).

1. Prepare the configuration file _rapo.ini_ according to the documentation.

3. Create a special file called _scheduler.py_ with `rapo.Scheduler()` declared and execute it as follows `python scheduler.py start`.

## Usage
Prepare your controls using the configuration table as described in the documentation.
