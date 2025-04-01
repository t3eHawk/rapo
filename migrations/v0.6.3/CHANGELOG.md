# Rapo v0.6.3 Change Log

## Annotatio
This is a new release with a lot of new features, bug fixes and improvements.

1. **Reconciliations**. This type of controls has been reworked; now it checks the completeness of data in two sources to identify losses and discrepancies. The old type of Reconciliation controls with the subtype Matching has been renamed to the **Comparison** control type.
1. **Parallelism** as a new parameter of controls provides the ability to choose the number of processes that execute control queries in the database. Remember that this parameter should not be specified thoughtlessly as it significantly affects the wear and tear of database resources. It is not recommended to set a value higher than 4.
1. **Time Windows** expands the capabilities of selecting data in controls over a specific time interval. Now data in control can be collected for any number of time periods, whether it is a day, week or month.
1. **Timeout** as a new parameter allows you to set a time limit within which control can be executed and after which it will be canceled with status C. Currently, this is supported only for control runs via API or the `launch()` method, but not through the scheduler or basic interface, i.e. the `run()` method.
1. **Iterations** allows the same control to be executed with different settings. Currently, only **Time Windows** control parameters are supported.
1. The new control parameter **Completion SQL** allows to perform an action upon control completion, similar to **Preparation SQL**.
1. Control subtypes are no longer relevant and have been removed from control properties and system structure.
1. New built-in case types have been added: Success, Loss, Duplicate.
1. The interface name to work with API and GUI has been simplified.
1. Logging to the console has been disabled in API server operations to avoid encoding issues in some environments.
1. The method for cleaning up temporary tables has been changed, which are now irrevocably deleted.
1. Fixed an issue with performing analytical controls when more than 9 cases were configured in the control.
1. Some parameters and functions have been laid out for future innovations and changes.

## Important Changes
This release brings some notable database schema and configuration changes described below.

### Database
In the `RAPO_CONFIG` table, new fields have been added:
| Field Name           | Content Type | Default Value | Short Description                          |
|----------------------|--------------|---------------|--------------------------------------------|
| `source_key_field_a` | String       |               | Name of the field with the unique key of source A. |
| `source_type_a`      | String       |               | Type of source A used for additional grouping of controls. |
| `source_key_field_b` | String       |               | Name of the field with the unique key of source B. |
| `source_type_b`      | String       |               | Type of source B used for additional grouping of controls. |
| `period_number`      | Number       | 1             | Number of time intervals included in the control sample. |
| `period_type`        | String       | D             | Type of time interval in the control sample: D (day), M (month), W (week). |
| `iteration_config`   | JSON         |               | Table of settings for control iterations. |
| `timeout`            | Number       |               | Control execution time limit.               |
| `instance_limit`     | Number       | 1             | Future functionality. Limiting the number of instances of the same control running simultaneously. |
| `output_limit`       | Number       |               | Future functionality. Limiting the number of records saved in control result tables. |
| `parallelism`        | Number       | 4             | Number of parallel processes executing database queries. |

In the `RAPO_CONFIG` table, fields have been renamed:
| Field Name            | Previous Name   |
|-----------------------|-----------------|
| `control_description` | `control_desc`  |
| `case_definition`     | `result_config` |
| `error_definition`    | `error_config`  |
| `schedule_config`     | `schedule`      |
| `period_back`         | `days_back`     |

In the `RAPO_LOG` table, fields have been renamed:
| Field Name         | Previous Name |
|--------------------|---------------|
| `fetched_number`   | `fetched`     |
| `success_number`   | `success`     |
| `error_number`     | `errors`      |
| `fetched_number_a` | `fetched_a`   |
| `fetched_number_b` | `fetched_b`   |
| `success_number_a` | `success_a`   |
| `success_number_b` | `success_b`   |
| `error_number_a`   | `errors_a`    |
| `error_number_b`   | `errors_b`    |

* The `RAPO_REF_CASES` directory has been added with records: Success, Loss, Duplicate.
* The `RAPO_REF_SUBTYPES` directory has been removed.

### Configuration
#### Rule Configuration
The main configuration of reconciliation rules is stored as a JSON structure populated in the `rule_config` field:
```json
{
    "need_issues_a": true,
    "need_issues_b": true,
    "need_recons_a": false,
    "need_recons_b": false,
    "allow_duplicates": false,
    "time_shift_from": -10,
    "time_shift_to": 10,
    "time_tolerance_from": -5,
    "time_tolerance_to": 5,
    "correlation_config": [
        {
            "field_a": "key_field_name_a_1",
            "field_b": "key_field_name_b_1",
            "allow_null": false
        },
        {
            "field_a": "key_field_name_a_2",
            "field_b": "key_field_name_b_2",
            "allow_null": false
        },
        ...
        {
            "field_a": "key_field_name_a_n",
            "field_b": "key_field_name_b_n",
            "allow_null": false
        }
    ],
    "discrepancy_config": [
        {
            "field_a": "numeric_field_name_a_1",
            "field_b": "numeric_field_name_b_1",
            "numeric_tolerance_from": -10,
            "numeric_tolerance_to": 10,
            "percentage_mode": false
        },
        {
            "field_a": "numeric_field_name_a_2",
            "field_b": "numeric_field_name_b_2",
            "numeric_tolerance_from": -5,
            "numeric_tolerance_to": 5,
            "percentage_mode": true
        },
        ...
        {
            "field_a": "numeric_field_name_a_n",
            "field_b": "numeric_field_name_b_n",
            "numeric_tolerance_from": 0,
            "numeric_tolerance_to": 0,
            "percentage_mode": false
        }
    ]
}
```

#### Time Windows
**Time Windows** are configured using the control parameters `period_back`, `period_number`, `period_type`, which allows flexible data selection in control for typical analytical time intervals. The table presents various combinations of these parameters with an explanation of the final sample.
<table>
  <tr>
    <th>Parameter</th>
    <th>Value</th>
    <th>Comment</th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>1</td>
    <td rowspan="3">Standard settings for daily controls when data is selected in control for the previous day.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>1</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>D</td>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>3</td>
    <td rowspan="3">Selecting data in control for the two days preceding the previous day.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>2</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>D</td>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>1</td>
    <td rowspan="3">Selecting data in control for the past actual week, i.e., starting from the day that was a week ago from the current date.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>1</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>W</td>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>1</td>
    <td rowspan="3">Selecting data in control for the previous calendar month.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>1</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>M</td>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>0</td>
    <td rowspan="3">Selecting data in control for the current calendar month.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>1</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>M</td>
  </tr>
  <tr>
    <th></th>
    <th></th>
    <th></th>
  </tr>
  <tr>
    <td>PERIOD_BACK</td>
    <td>3</td>
    <td rowspan="3">Selecting data in control for the month that was three months ago.</td>
  </tr>
  <tr>
    <td>PERIOD_NUMBER</td>
    <td>1</td>
  </tr>
  <tr>
    <td>PERIOD_TYPE</td>
    <td>M</td>
  </tr>
</table>

#### Iteration Configuration
Iterations are executed one after another immediately after the main control execution according to the schedule exactly as many times as the number of iterations configured.

When adding an iteration, a valid sequential `iteration_id` must be specified, and an optional parameter `iteration_description` can be filled in for the description. The relevance of the iteration can be configured using the `status` parameter, which accepts values `Y` or `N`.

Iterated control parameters have the same names as the main ones and must be filled in using the appropriate data types. Currently, only **Time Period** settings are supported.

Iterations configuration is represented as a JSON structure populated in the `iteration_config` field:
```json
[
    {
        "iteration_id": 1,
        "iteration_description": null,
        "period_back": 2,
        "period_number": 1,
        "period_type": "D",
        "status": "Y"
    },
    {
        "iteration_id": 2,
        "iteration_description": null,
        "period_back": 3,
        "period_number": 1,
        "period_type": "D",
        "status": "N"
    },
    {
        "iteration_id": 3,
        "iteration_description": null,
        "period_back": 1,
        "period_number": 1,
        "period_type": "M",
        "status": "Y"
    },
    ...
    {
        "iteration_id": n,
        "iteration_description": null,
        "period_back": 30,
        "period_number": 1,
        "period_type": "D",
        "status": "Y"
    }
]
```

### API
Some requests now return modified structures due to schema changes and new features.
See the API documentation for the following requests:
`get-all-controls`,
`get-control-run`,
`get-control-runs`,
`get-control-versions`,
`get-running-controls`.


---
See commits of this release [here](https://github.com/t3eHawk/rapo/compare/v0.5.1...v0.6.3).
