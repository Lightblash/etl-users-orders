from configparser import ConfigParser
from sqlalchemy import create_engine


def get_sql_engine(config_file):
    """
        Function parse configuration file and returns sqlalchemy engine for
        connecting to postgres database

        Parameters
        ----------

        config_file: str
            Path to config file with the following structure:

            [Postgres]
            username = <USERNAME>
            password = <PASSWORD>
            host = <HOST>
            port = <PORT>
            database = <DATABASE_NAME>

        Return
        ------

        sqlalchemy.Engine instance
    """

    CONNECTION_OPTIONS_LIST = [
        'host',
        'port',
        'database',
        'username',
        'password'
    ]

    cfg_parser = ConfigParser()
    read_configs = cfg_parser.read(config_file)

    if not read_configs:
        raise FileNotFoundError(f"No such file: {config_file}")

    if cfg_parser.has_section('Postgres'):
        if not set(CONNECTION_OPTIONS_LIST).issubset(
                cfg_parser.options('Postgres')):
            raise ValueError(
                "Connection options not found in config file in"
                " Postgres section. Check structure of config_example.ini"
            )
        else:
            pg_username = cfg_parser['Postgres']['Username']
            pg_password = cfg_parser['Postgres']['Password']
            pg_host = cfg_parser['Postgres']['Host']
            pg_port = cfg_parser['Postgres']['Port']
            pg_database = cfg_parser['Postgres']['Database']

            return create_engine(
                f'postgresql+psycopg2://{pg_username}:{pg_password}@'
                f'{pg_host}:{pg_port}/{pg_database}'
            )
    else:
        raise ValueError("Postgres section not found in config file. Check "
                         "structure of config in config_example.ini")
