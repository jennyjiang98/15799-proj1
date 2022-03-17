def task_project1_setup():
    return {
        "actions": [
            'sudo apt update',
            'sudo apt -y install postgresql-14-hypopg',
            'sudo apt -y install python3-pip',
            'sudo pip3 install sql_metadata',
            'sudo pip3 install pglast',
            'sudo pip3 install pandas',
            'sudo pip3 install pandarallel',
            'sudo pip3 install tqdm',
            'sudo pip3 install psycopg2-binary',
            'sudo pip3 install psycopg2',
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
            'echo \'{"VACUUM": false}\' > config.json',
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
