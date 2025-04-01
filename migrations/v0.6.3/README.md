# Rapo v0.6.3 Migration Instructions
This short document describes how to upgrade Rapo from v0.5.1 to v0.6.3:

1. Wait until all your Rapo controls are completed or cancel them. Stop the scheduler.
    ```python
    import rapo


    scheduler = rapo.Scheduler()
    scheduler.stop()
    ```
1. If you use API or GUI initiated for instance by the `server.py` file then stop the server.
    ```bash
    python server.py stop
    ```
1. Modify `server.py` content to have a working name of API class:
    ```python
    import rapo


    server = rapo.Server()
    ```
1. Backup your control configurations.
    ```sql
    create table rapo_config_v_0_5_1 as
    select * from rapo_config;
    ```
1. Perform module upgdate `pip install --upgrade rapo==0.6.3`.
1. Execute migration SQL [scripts](upgrade.sql) in database, which include:
    1. Control configuration table migration to add and update parameters.
    1. Control run history table migration to update properties.
1. Test your controls and restore Rapo processes.
