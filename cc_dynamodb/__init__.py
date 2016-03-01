import copy
import os

from boto import dynamodb2
from boto.dynamodb2 import table
from bunch import Bunch
from hcl_translator import dynamodb2_translator


from .exceptions import ConfigurationError
from .log import create_logger


logger = create_logger()
UPDATE_INDEX_RETRIES = 60

# Cache to avoid parsing config file repeatedly.
_cached_config = None
_dynamodb_translator = None


def set_config(dynamodb_tf, namespace=None, aws_access_key_id=False, aws_secret_access_key=False,
               host=None, port=None, is_secure=None):
    global _cached_config
    global _dynamodb_translator

    _dynamodb_translator = dynamodb2_translator(dynamodb_tf, logger)

    _cached_config = Bunch({
        'namespace': namespace or os.environ.get('CC_DYNAMODB_NAMESPACE'),
        'aws_access_key_id': aws_access_key_id or os.environ.get('CC_DYNAMODB_ACCESS_KEY_ID', False),
        'aws_secret_access_key': aws_secret_access_key or os.environ.get('CC_DYNAMODB_SECRET_ACCESS_KEY', False),
        'host': host or os.environ.get('CC_DYNAMODB_HOST'),
        'port': port or os.environ.get('CC_DYNAMODB_PORT'),
        'is_secure': is_secure or os.environ.get('CC_DYNAMODB_IS_SECURE'),
    })

    if not _cached_config.namespace:
        msg = 'Missing namespace kwarg OR environment variable CC_DYNAMODB_NAMESPACE'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_access_key_id is False:
        msg = 'Missing aws_access_key_id kwarg OR environment variable CC_DYNAMODB_ACCESS_KEY_ID'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.aws_secret_access_key is False:
        msg = 'Missing aws_secret_access_key kwarg OR environment variable CC_DYNAMODB_SECRET_ACCESS_KEY'
        logger.error('ConfigurationError: ' + msg)
        raise ConfigurationError(msg)
    if _cached_config.port:
        try:
            _cached_config.port = int(_cached_config.port)
        except ValueError:
            msg = ('Integer value expected for port '
                   'OR environment variable CC_DYNAMODB_PORT. Got %s' % _cached_config.port)
            logger.error('ConfigurationError: ' + msg)
            raise ConfigurationError(msg)

    logger.info('cc_dynamodb.set_config', extra=dict(status='config loaded'))


def get_config():
    global _cached_config
    if not _cached_config:
        raise ConfigurationError('set_config must be called before get_config')
    return Bunch(copy.deepcopy(_cached_config.toDict()))


def get_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    return get_config().namespace + table_name


def get_reverse_table_name(table_name):
    '''Prefixes the table name for the different environments/settings.'''
    prefix_length = len(get_config().namespace)
    return table_name[prefix_length:]


def get_table_index(table_name, index_name):
    if _dynamodb_translator is None:
        raise ConfigurationError('set_config must be called')
    return _dynamodb_translator.get_table_index(table_name, index_name)


def get_connection():
    """Returns a DynamoDBConnection even if credentials are invalid."""
    config = get_config()

    if config.host:
        from boto.dynamodb2.layer1 import DynamoDBConnection
        return DynamoDBConnection(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            host=config.host,                           # Host where DynamoDB Local resides
            port=config.port,                           # DynamoDB Local port (8000 is the default)
            is_secure=config.is_secure or False)        # For DynamoDB Local, disable secure connections

    return dynamodb2.connect_to_region(
        'us-west-2',
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
    )


def list_table_names():
    """List known table names from configuration, without namespace."""
    if _dynamodb_translator is None:
        raise ConfigurationError('set_config must be called')
    return _dynamodb_translator.list_table_names()


def get_table(table_name, connection=None):
    '''Returns a dict with table and preloaded schema, plus columns.

    WARNING: Does not check the table actually exists. Querying against
             a non-existent table raises boto.exception.JSONResponseError

    This function avoids additional lookups when using a table.
    The columns included are only the optional columns you may find in some of the items.
    '''
    if _dynamodb_translator is None:
        raise ConfigurationError('set_config must be called')
    return table.Table(
        get_table_name(table_name),
        connection=connection or get_connection(),
        **_dynamodb_translator.table_kwargs(table_name)
    )


def list_tables(connection=None):
    dynamodb = connection or get_connection()
    return dynamodb.list_tables()
