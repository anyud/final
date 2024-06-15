def create_connection(config):
    db_user = config['db_user']
    db_password = config['db_password']
    db_host = config['db_host']
    db_port = config['db_port']
    db_name = config['db_name']
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    return connection_string
