import botocore.session
from boto.dynamodb2.types import Dynamizer
from tornado import gen
from tornado_botocore import Botocore


class DynamoDBConnectionError(Exception):
    pass


class DynamoDBWithReplication(object):
    DEFAULT_REGION       = 'us-west-1'
    DEFAULT_REPL_REGIONS = ['us-west-1', 'eu-central-1']

    def __init__(self, session=None, regions_list=DEFAULT_REPL_REGIONS, default_region=DEFAULT_REGION, *args, **kwargs):
        self.default_region = default_region
        self.regions_list = regions_list
        for region in self.regions_list:
            setattr(self, 'ddb-%s' % region, DynamoDB(session, region_name=region, *args, **kwargs))

    @gen.coroutine
    def get(self, *args, **kwargs):
        db_instance = getattr(self, 'ddb-%s' % self.default_region)
        result = yield db_instance.get(*args, **kwargs)
        raise gen.Return(result)

    @gen.coroutine
    def put(self, *args, **kwargs):
        futures = []
        for i, region in enumerate(self.regions_list):
            db_instance = getattr(self, 'ddb-%s' % region)
            futures.append(db_instance.put(*args, **kwargs))
        try:
            yield gen.multi_future(futures)
        except Exception, e:
            self._process_multiple_exceptions(futures)

    @gen.coroutine
    def delete(self, *args, **kwargs):
        # No need to send DELETE to all regions. AWS replication will do this in a second.
        db_instance = getattr(self, 'ddb-%s' % self.default_region)
        yield db_instance.get(*args, **kwargs)

    def _process_multiple_exceptions(self, futures):
        # If key exists in all regions, than it is Duplicationo Error. But if key exists in some regions, it means
        # that Dynamo replication was faster than async put method.
        failed_futures_list = []
        for f in futures:
            try:
                f.result()
            except Exception as e:
                if f.done():
                    failed_futures_list.append(f)
                    exception = e
        if len(failed_futures_list) == len(self.regions_list):
            raise exception
        else:
            pass


class DynamoDB(object):
    SCHEMA          = [{'AttributeName': 'Id',
                        'KeyType': 'HASH'}]
    ATTRIBUTE_DEFN = [{'AttributeName': 'Id',
                        'AttributeType': 'S',}]
    PROVISIONEDTHROUGHPUT = {'ReadCapacityUnits': 5,
                             'WriteCapacityUnits': 5}

    def __init__(self, session, access_key=LOGGER_KEY, secret_key=LOGGER_SECRET_KEY,
                 region_name=LOGGER_REGION, connect_timeout=None, request_timeout=None, endpoint_url=DYNAMODB_ENDPOINT):
        self.region_name = region_name
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.endpoint_url = endpoint_url
        self.session = session or botocore.session.get_session()
        if not self.session.get_credentials():
            self.session.set_credentials(access_key, secret_key)

    @gen.coroutine
    def get(self, table_name, key):
        encoded_key = self._encode_item(key)
        dynamodb_get_item = self._create_dynamodb_task('GetItem')
        item = yield self._run_dynamodb_task(
            dynamodb_get_item.call,
            TableName=table_name,
            Key=encoded_key,
            ConsistentRead=False
        )
        if item: item = self._decode_item(item)
        raise gen.Return(item)

    @gen.coroutine
    def put(self, table_name, item, overwrite=False):
        encoded_item = self._encode_item(item)
        dynamodb_put_item = self._create_dynamodb_task('PutItem')
        kwargs = dict(
                    TableName=table_name,
                    Item=encoded_item)
        if not overwrite:
            kwargs.update(dict(Expected=self._get_expected()))
        yield self._run_dynamodb_task(dynamodb_put_item.call, **kwargs)


    @gen.coroutine
    def delete(self, table_name, item):
        encoded_item = self._encode_item(item)
        dynamodb_delete_item = self._create_dynamodb_task('DeleteItem')
        yield self._run_dynamodb_task(
            dynamodb_delete_item.call,
            TableName=table_name,
            Key=encoded_item
        )
        # optionally it is possyble to get some params from key before deleting

    @gen.coroutine
    def _create_table(self, **kwargs):
        dynamodb_create_table = self._create_dynamodb_task('CreateTable')
        table_name = kwargs.get('TableName')
        yield gen.Task(
            dynamodb_create_table.call,
            AttributeDefinitions=self.ATTRIBUTE_DEFN,
            KeySchema=self.SCHEMA,
            ProvisionedThroughput=self.PROVISIONEDTHROUGHPUT,
            TableName=table_name
        )

    def _create_dynamodb_task(self, task, region=None):
        return Botocore(
            service='dynamodb',
            operation=task,
            region_name=region or self.region_name,
            endpoint_url=self.endpoint_url,
            session=self.session,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout
        )


    @gen.coroutine
    def _run_dynamodb_task(self, task, **kwargs):
        try:
            result = yield gen.Task(task, **kwargs)
            try:
                self._check_errors(result)
            except:
                yield self._create_table(**kwargs)
                result = yield gen.Task(task, **kwargs)
                self._check_errors(result)
        raise gen.Return(result.get('Item'))

    def _encode_item(self, item):
        item_cp = item.copy()
        for key, val in item_cp.iteritems():
            item_cp.update({key: Dynamizer().encode(val)})
        return item_cp

    def _decode_item(self, encoded_item):
        encoded_item_cp = encoded_item.copy()
        # only 1-depth supported.
        # TODO: provide n-depth of dict
        for key, val in encoded_item_cp.iteritems():
            encoded_item_cp.update({key: Dynamizer().decode(val)})
        return encoded_item_cp

    def _get_expected(self):
        return {'_id': {'Exists': False}}

    def _check_errors(self, resp_dict):
        if 'Error' in resp_dict:
            raise HTTPError(resp_dict['ResponseMetadata']['HTTPStatusCode'], resp_dict['Error']['Message'])

