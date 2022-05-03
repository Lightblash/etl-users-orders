import pandas as pd
from argparse import ArgumentParser
from sqlalchemy.types import Integer
import common
import logging
import os

# schema name in Postgres
SCHEMA_NAME = 'selectel_test'


def get_data_from_file(file):
    """
        Read data from source csv file into pandas DataFrame
    """

    logging.info('Start Extract Session')
    df = pd.read_csv(file, sep='\t')
    logging.info(f'Extract Completed. Number of rows in the file: {df.shape[0]}')
    # check for null values in price col
    if df['price'].isnull().sum() > 0:
        logging.warning(
            f'Number of rows with empty price: {df[df.price.isnull()].shape[0]}')
    return df


def process_df(df):
    """
        Process source dataframe
    """

    logging.info('Start Transformation Session')

    # remove duplicated column
    df.drop(columns=['user_id.1'], inplace=True)

    # transform datetime columns into correct data type
    df['service_start_date'] = pd.to_datetime(
        df['service_start_date'], format='%d.%m.%Y')
    df['service_end_date'] = pd.to_datetime(
        df['service_end_date'], format='%d.%m.%Y')

    # drop full duplicates if exists
    df.drop_duplicates(inplace=True)

    logging.info('Transformation completed. Number of rows after transformation:'
                 f' {df.shape[0]}')

    return df


def load_users_data(processed_df, sql_engine):
    """
        Load users data to postgres table
    """

    logging.info('Start Loading Users Data')

    # get only users related columns
    users_df = processed_df[
        ['user_id', 'user_name', 'user_surname']].drop_duplicates()

    users_df.rename(
        columns={
            'user_id': 'id',
            'user_name': 'name',
            'user_surname': 'surname'
        }, inplace=True)

    users_df.to_sql(
        name='users',
        con=sql_engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        method='multi'
    )

    logging.info(f'Users Data Loaded. {users_df.shape[0]} rows were inserted')


def load_services_data(processed_df, sql_engine):
    """
        Load services data to postgres table
    """

    logging.info('Start Loading Services Data')
    services_df = processed_df[['service_id']].drop_duplicates().rename(
        columns={'service_id': 'id'})

    services_df.to_sql(
        name='services',
        con=sql_engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        method='multi'
    )

    logging.info(
        f'Services Data Loaded. {services_df.shape[0]} rows were inserted')


def load_server_configs_data(processed_df, sql_engine):
    """
        Load server configs data to postgres table
    """

    logging.info('Start Loading Server Configs Data')
    server_configs_df = processed_df[
        ['server_configuration']].drop_duplicates().rename(
            columns={'server_configuration': 'id'})

    server_configs_df.to_sql(
        name='server_configs',
        con=sql_engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        method='multi'
    )

    logging.info(f'Server Configs Data Loaded. {server_configs_df.shape[0]} '
                 f'rows were inserted')


def load_orders_data(processed_df, sql_engine):
    """
        Load orders data to postgres table
    """
    logging.info('Start Loading Orders Data')
    orders_df = processed_df[[
        'server_order_id',
        'user_id',
        'service_id',
        'server_configuration',
        'service_start_date',
        'service_end_date',
        'price']]

    orders_df = orders_df.rename(columns={'server_order_id': 'id'})

    orders_df.to_sql(
        name='orders',
        con=sql_engine,
        schema=SCHEMA_NAME,
        if_exists='append',
        index=False,
        method='multi',
        dtype={'price': Integer()}
    )

    logging.info(f'Orders Data Loaded. {orders_df.shape[0]} rows were inserted')


def parse_cmd_args():
    """
        Parse command line arguments
    """

    parser = ArgumentParser(
        description='ETL pipeline is used for deliver orders data from csv file'
                    ' to Postgres DB tables',
        epilog='If any questions, pls contact aro'
    )

    parser.add_argument(
        '--db_config',
        help='full path to config file. Structure of config file must be the '
             'the same as in config_example.ini. Default path=config.ini',
        default='config.ini'
    )

    parser.add_argument(
        '--csv_file',
        help='full path to file with data. Default=cf_test_dataset.csv',
        default='cf_test_dataset.csv'
    )

    parser.add_argument(
        '--first_run',
        help='Add this argument for the first script run. SQL query in '
             'schema.sql will be executed',
        action='store_true'
    )

    parser.add_argument(
        '--log',
        help='Full path to log file. Default is None',
        default=None
    )

    parser.add_argument(
        '--log-level',
        help='Log level. Default is info',
        default='info',
        choices=['debug', 'info', 'warning', 'error', 'critical']
    )

    args = parser.parse_args()

    return args


def main():
    args = parse_cmd_args()

    logging.basicConfig(
        filename=args.log,
        format='%(asctime)s : %(levelname)s : %(message)s',
        level=getattr(logging, args.log_level.upper()),
        filemode='a'
    )

    logging.info(f"Script start : {os.path.basename(__file__)}")

    sql_engine = common.get_sql_engine(args.db_config)

    if args.first_run:
        schema_sql_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'sql')
        sql_file = os.path.join(schema_sql_dir, 'schema.sql')
        with open(sql_file, 'r') as file:
            sql_create_query = file.read()
        with sql_engine.connect() as conn:
            conn.execute(sql_create_query)

    df = get_data_from_file(args.csv_file)

    processed_df = process_df(df)

    load_users_data(processed_df, sql_engine)

    load_services_data(processed_df, sql_engine)

    load_server_configs_data(processed_df, sql_engine)

    load_orders_data(processed_df, sql_engine)

    logging.info("Done!")


if __name__ == '__main__':
    main()
