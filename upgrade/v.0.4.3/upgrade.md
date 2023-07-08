# RAPO v0.4.0 Upgrade Instructions
This short document describes how to upgrade RAPO from v0.3.3 to v0.4.3:

1. Perform module upgdate `pip install --upgrade rapo`
1. Execute migration SQL [scripts](upgrade.sql) in database, which include:
    1. Creation of new RAPO tables (dictionaries) and triggers.
    1. Control configuration table migration with new fields and data type updates.
    1. Control run history table migration with new fields.
    1. Control result tables migration with new mandatory fields in Analysis controls.
    1. Component tables migration.
1. Replace HOOK functions with new names but the old body. It must be now function `RAPO_PRERUN_CONTROL_HOOK` and procedure `RAPO_POSTRUN_CONTROL_HOOK`.
1. Modify configuration file as in the example below.

## Configuration File
This example shows how your _rapo.ini_ should now look like:
```
[DATABASE]
vendor_name=oracle
driver_name=cx_oracle
client_path=/path/to/oracle/client/bin
host=example-db-server
port=1234
sid=exampledb
username=exampleuser
password=********
max_identifier_length=128
max_overflow=5
pool_pre_ping=True
pool_size=20
pool_recycle=-1
pool_timeout=30

[LOGGING]
console=True
file=True
info=True
debug=True
error=True
warning=True
critical=True

[API]
host=example-application-server
port=12345
token=SOMETOKEN123
```
