import subprocess
import time

def wait_for_postgres(host, max_retries=5, delay_second=5):
    retries = 0

    while retries<max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )

            if "accepting_connections" in result.stdout:
                print("Successfully connected to postgres")
                return True

        except subprocess.CalledProcessError as e:
            retries+=1
            time.sleep(delay_second)
    print("Max tries reached")
    return False

if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT Script....")

source_config = {
    "dbname": "source_db",
    "user": 'temporal',
    'password': 'temporal',
    'host': 'source_postgres'
}

destination_config = {
    'dbname': "destination_db",
    'user': 'temporal',
    'password': 'temporal',
    'host': 'destination_postgres'
}

dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'
]

subprocess_env = dict(PGPASSWORD = source_config['password'])

subprocess.run(dump_command, env=subprocess_env, check=True)


load_commands = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql',
]

subprocess_env = dict(PGPASSWORD = source_config['password'])
subprocess.run(load_commands, env=subprocess_env, check=True)

print("Ending ELT Script...")
