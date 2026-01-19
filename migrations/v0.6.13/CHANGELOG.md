# Rapo v0.6.13 Change Log

## Annotatio
This is a new release with a lot of new features, bug fixes and improvements.

1. **Reconciliations**. This type of controls has been reworked; now it checks the completeness of data in two sources to identify losses and discrepancies. The old type of Reconciliation controls with the subtype Matching has been renamed to the **Comparison** control type.
1. **Parallelism** as a new parameter of controls provides the ability to choose the number of processes that execute control queries in the database. Remember that this parameter should not be specified thoughtlessly as it significantly affects the wear and tear of database resources. It is not recommended to set a value higher than 4.
1. **Time Windows** expands the capabilities of selecting data in controls over a specific time interval. Now data in control can be collected for any number of time periods, whether it is a day, week or month.
1. **Timeout** as a new parameter allows you to set a time limit within which control can be executed and after which it will be canceled with status C. Currently, this is supported only for control runs via API or the `launch()` method, but not through the scheduler or basic interface, i.e. the `run()` method.
1. **Instance Limit** as a new parameter allows you to control the number of instances of the same control running simultaneously. Currently, it is only supported when running control through the `launch()` method.
1. **Output Limit** as a new parameter, allows you to set a limit on the number of records stored in control result tables. Currently, it only works for reconciliations.
1. **Iterations** allows the same control to be executed with different settings. Currently, only **Time Windows** control parameters are supported.
1. **Cascades** are a new way of planning controls that allow automation of control execution in chains, where only the first control has a scheduled start time, while the rest launch immediately after the previous one is completed.
1. The new control parameter **Completion SQL** allows to perform an action upon control completion, similar to **Preparation SQL**.
1. Now a field with the `TIMESTAMP` type can be specified as the date field for control. Important: during control execution, the field will be converted to the `DATE` type and stored in this form in the control result tables.
1. New parameters have been added that contain the names of fields with unique keys for data sources. If the source is a simple *table* without the specified field, the table's `ROWID` is automatically used as the key, and the specified name is used as the alias in the control result table. If the source is a *view*, the field with the specified name must contain a pre-prepared unique key.
1. **Debug Mode** is a new control state with a special execution way in which temporary tables are not deleted.
1. New methods added for deleting and clearing control tables. It is now possible to delete temporary tables related to a specific control process, as well as delete or clear result tables of a particular control.
1. Naming conventions and the approach to control temporary tables updated. Controls now have mandatory temporary input tables called `SOURCE`, as well as output tables `ERROR` and `STAGE`, which store negative and positive/neutral control results, respectively.
1. New parameters have been added that contain the types of data sources in the control for additional grouping.
1. The `STARTED` status now reflects not only the actual start of control process, but also the execution of Preparation SQL and the calculation of the Prerequisite Value. A new status, `WAITING`, now represents the brief moment between a control leaving the process queue and the beginning of its execution.
1. Control subtypes are no longer relevant and have been removed from control properties and system structure.
1. New built-in case types have been added: Success, Loss, Duplicate.
1. The interface name to work with API and GUI has been simplified.
1. Logging to the console has been disabled in API server operations to avoid encoding issues in some environments.
1. Result tables will now be created even if there are no results to save for all types of controls.
1. The method for cleaning up temporary tables has been changed, which are now irrevocably deleted.
1. Fixed an issue with performing analytical controls when more than 9 cases were configured in the control.
1. Some parameters and functions have been laid out for future innovations and changes.
1. UI has undergone major changes both visually and functionally to enhance the user experience, fix bugs, and support reconciliations and other new features.

## Important Changes
This release brings some notable database schema and configuration changes described below.

### Features
This release contains a host of innovations, which are mainly concentrated around a new type of control - reconciliation.

#### Rule Configuration
The main configuration of reconciliation rules is stored as a JSON structure populated in the `rule_config` field:
```json
{
    "need_issues_a": true,
    "need_issues_b": true,
    "need_recons_a": false,
    "need_recons_b": false,
    "allow_duplicates": false,
    "fuzzy_optimization": true,
    "normalization_type": "default",
    "discrepancy_matching": false,
    "time_shift_from": -10,
    "time_shift_to": 10,
    "time_tolerance_from": -5,
    "time_tolerance_to": 5,
    "output_limit_a": null,
    "output_limit_b": null,
    "correlation_limit": false,
    "correlation_config": [
        {
            "field_a": "key_field_a_1",
            "field_b": "key_field_b_1",
            "allow_null": false,
            "formula_mode": false
        },
        {
            "field_a": "key_field_a_2",
            "field_b": "key_field_b_2",
            "allow_null": false,
            "formula_mode": false
        },
        {
            "field_a": "key_field_a_3",
            "field_b": "key_field_b_3",
            "allow_null": false,
            "formula_mode": false
        },
        {
            "field_a": "trim(key_field_a_4)",
            "field_b": "trim(key_field_b_4)",
            "allow_null": false,
            "formula_mode": true
        }
    ],
    "discrepancy_config": [
        {
            "field_a": "numeric_field_a_1",
            "field_b": "numeric_field_b_1",
            "numeric_tolerance_from": -10,
            "numeric_tolerance_to": 10,
            "percentage_mode": false,
            "formula_mode": false
        },
        {
            "field_a": "numeric_field_a_2",
            "field_b": "numeric_field_b_2",
            "numeric_tolerance_from": -10,
            "numeric_tolerance_to": 10,
            "percentage_mode": false,
            "formula_mode": false
        },
        {
            "field_a": "numeric_field_a_3",
            "field_b": "numeric_field_b_3",
            "numeric_tolerance_from": -5,
            "numeric_tolerance_to": 5,
            "percentage_mode": true,
            "formula_mode": false
        },
        {
            "field_a": "a.numeric_field_a_1+a.numeric_field_a_2",
            "field_b": "b.numeric_field_b_1+b.numeric_field_b_2",
            "numeric_tolerance_from": -10,
            "numeric_tolerance_to": 10,
            "percentage_mode": false,
            "formula_mode": true,
            "formula_alias": "numeric_field_1_2_sum"
        }
    ]
}
```

##### Allow Duplicates
A reconciliation parameter that allows to ignore records identified as duplicates during algorithm execution when saving results.

Duplicates are repeating records that have the same correlation key values on one side of the control, including the date key with Time Shift taken into account.

This parameter is useful when it is normal for the data source to contain duplicates on at least one side, and processing them among the results leads to false statistics and conclusions. Thus, if a record is lost or contains a discrepancy but is marked as a duplicate, it will not be considered as an error and will not be saved.
The number of records on each side is irrelevant, it can be 1:1, 1:2, 1:3, 2:3, etc.

##### Fuzzy Optimization
Reconciliation parameter that allows to join records within a correlation cluster of the same dimension (1k1, 2k2, 3k3, etc.) using a simple positional method with sorting by the sum of numerical values. Such clusters are formed as a result of conflicts if the correlation keys do not provide a unique connection and several records on one side are connected to several records on the other. This is a cheaper and faster way to match data, it is enabled by default, and is a standard step in the algorithm.

In rare cases, if the numerics have a strange distribution, you can disable this optimization, and then such conflicts will be resolved along with the rest by searching for the most similar pairs of records using the distance method.

It is set as the parameter `fuzzy_optimization`, default value is `true`, can be changed via a global parameter in the configuration file.

##### Normalization
A reconciliation parameter that allows normalization to be applied to numerics before calculating distances to resolve conflicts within correlation clusters. This is useful in cases where correlation keys do not guarantee a unique connection, while numerics have a narrow spread and can produce similar or even identical values when summed.

This is an engineering, test parameter that is hidden by default. It is recommended to activate it locally at the level of specific controls if the data sources are of low quality and it is impossible to select a set of correlation keys that would provide a unique connection. In such cases, a large number of correlation clusters and conflicts are generated, and the entire burden for connecting records falls on the search for the most similar pairs of records using the distance method. Therefore the values must be normalized to obtain the most accurate mathematical processing.

It is set as the parameter `normalization_type` with possible options: **default**, **none**, **minmax**, **rank**, **z_norm**. The default value is **none** (can be specified as **default**), can be changed via a global parameter in the configuration file.

##### Discrepancy Matching
A reconciliation parameter that allows to modify error mapping and classify duplicates as losses if there is a numeric discrapancy with the record on the other side of the control.

It is useful to use this extension together with the *Allow Duplicates* feature so that you do not lose duplicates that are actually errors from the results, but only ignore those that are random or successfully confirmed during the control.

It is set as the parameter `discrepancy_matching=true`, default value is `false`, can be changed via a global parameter in the configuration file.

##### Correlation Limit
Reconciliation parameter that helps protect against the issue of Cartesian joins resulting from weak or insufficient correlation configuration of data sources in control rules.
When this parameter is enabled, the result of data source correlation will be limited to prevent the generation of an excessive number of records.

|         |                                                                    |
|---------|--------------------------------------------------------------------|
| true    | The algorithm will automatically calculate the maximum number of correlation records using the formula max(fetched_number_a, fetched_number_b)*multiplier, where the multiplier is a constant equal to 2.5.|
| integer | The algorithm will use the specified value as the maximum number of correlation records.|
| false   | The algorithm will not apply any limits to the correlation.|

Enabling this parameter may slow down data correlation by up to 30%.
It is recommended to use it only during the initial stages of control development and testing.

##### Allow Null
Correlation parameter that determines whether records with null keys should be filtered out from the data sources.
By default, we assume that `allow_null=false` is the normal state, as a key should not be null.
However, if in your case a null key is expected as an error and you want to detect such cases, set `allow_null=true` for the corresponding field pair in the correlation settings.
This way, those records will no longer be filtered out and will appear as errors.

##### Formula Mode
A special mode for correlation keys and discrepancy determinants that allows formulas to be used as fields instead of ready-made data.
It is set as the parameter `formula_mode=true` individually for each pair of fields within the correlation or discrepancy configuration.

##### Percentage Mode
A special mode for discrepancy determinants, which calculates the discrepancy as a percentage rather than an absolute difference.
It is set as the parameter `percentage_mode=true`. The tolerance must be also specified by user as a percentage rather than a simple number.

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

#### Cascade Configuration
**Cascades** is a feature that defines a sequence of controls executed one after another. These controls can be linked either logically or through data, for example when one control uses the results of the previous control as its input. This creates a chain of dependent control that work together as a single process.

To configure cascades, a base control must be defined with a scheduled time, followed by a chain of controls, each linked by ID to the previous one in the chain via the special `trigger_id` parameter inside the `schedule_config` JSON field.

Controls in a cascade are executed unconditionally, one after another, even if the previous control fails with an error or returns no results.

Cascade execution is a part of the scheduler work and is not considered during manual control calls. If controls from a cascade need to be re-executed, each one is launched separately.

In the following example, Control #1 will start on schedule at 8:30. Then, once it completes, Control #2 will launch, while Control #3 will start immediately after Control #2 finishes.
<table>
    <tr>
        <th>CONTROL_ID</th>
        <th>SCHEDULE_CONFIG</th>
    </tr>
    <tr>
        <td>1</td>
        <td><pre><code class="json">{"mday": null, "wday": null, "hour": "8", "min": "30", "sec": "0"}</code></pre></td>
    </tr>
    <tr>
        <td>2</td>
        <td><pre><code class="json">{"mday": null, "wday": null, "hour": null, "min": null, "sec": null, "trigger_id": 1}</code></pre></td>
    </tr>
    <tr>
        <td>3</td>
        <td><pre><code class="json">{"mday": null, "wday": null, "hour": null, "min": null, "sec": null, "trigger_id": 2}</code></pre></td>
    </tr>
</table>

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
| `instance_limit`     | Number       | 1             | Limiting the number of instances of the same control running simultaneously. |
| `output_limit`       | Number       |               | Limiting the number of records saved in control result tables. |
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

**Reconciliations** can have up to two result tables (depending on the settings) with different prefixes in the name, A and B.
For instance, queries to the tables of the control named `TEST_CONTROL_123` will look as follows:
```sql
select * from rapo_resa_test_control_123;
select * from rapo_resb_test_control_123;
```
In these tables, in addition to the data source fields, a set of mandatory fields is populated.
<table>
  <tr>
    <th>Field Name</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>RAPO_RESULT_TYPE</td>
    <td>The type of case determined by the control for this record. Possible options: Loss, Duplicate, Discrepancy, as well as Success, if the control was configured to save reconsolidated records.</td>
  </tr>
  <tr>
    <td>RAPO_DISCREPANCY_ID</td>
    <td>Reference to the matched record from the opposite source, identified by the control as a record with a discrepancy in some factor. Populated only if result type is Discrepancy.</td>
  </tr>
  <tr>
    <td>RAPO_DISCREPANCY_DESCRIPTION</td>
    <td>Enumeration of factors for which discrepancies were identified in the control, with the discrepancy delta indicated in parentheses. Populated only if result type is Discrepancy.</td>
  </tr>
  <tr>
    <td>RAPO_PROCESS_ID</td>
    <td>Default reference on Rapo process ID that created this record.</td>
  </tr>
</table>

### Configuration
You can now assign a name to your application instance using the new parameter `instance_name` in the base **SCHEDULER** section:

```
[SCHEDULER]
instance_name=i_am_robot
```

A new section appears in the configuration file *rapo.ini* with global algorithm settings, which can be used to change the default values for all controls at once.

Each parameter in this section corresponds to the setting of the same name in the controls, but is limited by the list as in the example below.

```
[ALGORITHM]
fuzzy_optimization=True
normalization_type=default
discrepancy_matching=False
```

For correct configuration processing, this section must be added to the configuration file.

### API
Some requests now return modified structures due to schema changes and new features.
See the API documentation for the following requests:
`get-all-controls`,
`get-control-run`,
`get-control-runs`,
`get-control-versions`,
`get-running-controls`.

Now control tables can be managed with new commands:
`delete-control-output-tables`,
`delete-control-temporary-tables`.

### UI
The user interface significantly redesigned, several bug fixes applied.
Control views are now displayed as a table, control configuration format substantially updated, and support for reconciliations added.

---
See commits of this release [here](https://github.com/t3eHawk/rapo/compare/v0.5.1...v0.6.13).
