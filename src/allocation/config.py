import os


def get_postgres_uri():
    # host = os.environ.get('DB_HOST', 'localhost')
    host = '127.0.0.1'
    # port = 54321 if host == 'localhost' else 5432
    port = 5432
    # password = os.environ.get('DB_PASSWORD', 'abc123')
    password = 'root'
    user, db_name = 'postgres', 'allocations'
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_api_url():
    # host = os.environ.get('API_HOST', 'localhost')
    host = 'localhost'
    # port = 5005 if host == 'localhost' else 80
    port = 5005
    return f"http://{host}:{port}"
