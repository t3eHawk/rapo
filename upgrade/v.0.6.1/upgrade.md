# Rapo v0.6.1 Upgrade Instructions
This short document describes how to upgrade Rapo from v0.5.1 to v0.6.1:

1. Perform module upgdate `pip install --upgrade rapo==0.6.1`
1. Execute migration SQL [scripts](upgrade.sql) in database,, which include:
    1. Control configuration table migration to add and update parameters.
    1. Control run history table migration to update properties.
