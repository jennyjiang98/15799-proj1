def task_project1_setup():
    file = '/etc/postgresql/14/main/postgresql.conf'
    content = '\'max_connections = 200\nshared_buffers = 4GB\neffective_cache_size = 12GB\nmaintenance_work_mem = 1GB\n' \
              'checkpoint_completion_target = 0.9\nwal_buffers = 16MB\ndefault_statistics_target = 100\nrandom_page_cost = 1.1\n' \
              'effective_io_concurrency = 200\nwork_mem = 10485kB\nmin_wal_size = 1GB\nmax_wal_size = 4GB\nmax_worker_processes = 4\n' \
              'max_parallel_workers_per_gather = 2\nmax_parallel_workers = 4\nmax_parallel_maintenance_workers = 2\n\''

    return {
            "actions": [
                # 'sudo apt update',
                'sudo bash -c "echo ' + content + ' >> ' + file + '"',
                'sudo apt -y install postgresql-14-hypopg',
                'sudo apt -y install python3-pip',
                'pip3 install sql_metadata',
                'pip3 install pglast',
                'pip3 install pandas',
                'pip3 install pandarallel',
                'pip3 install tqdm',
                'pip3 install psycopg2-binary',
                'pip3 install psycopg2',
            ],
            "uptodate": [False],
        }


def task_project1():
    """
    Generate actions.
    """
    from tuning import generate
    return {
        # A list of actions. This can be bash or Python callables.
        "actions": [
            'echo "Starting action generation."',
            generate,
            'echo \'{"VACUUM": false, "RESTART": true}\' > config.json',
        ],
        # Always rerun this task.
        "uptodate": [False],
        "verbosity": 2,
        "params": [
            {
                "name": "workload_csv",
                "long": "workload_csv",
                "help": "The PostgreSQL workload to optimize for.",
                "default": None,
            },
            {
                "name": "timeout",
                "long": "timeout",
                "help": "The time allowed for execution before this dodo task will be killed.",
                "default": None,
            },
        ],
    }
