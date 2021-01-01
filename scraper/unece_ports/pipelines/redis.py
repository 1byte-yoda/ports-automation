import json
from scrapy.exceptions import NotConfigured
from twisted.internet import defer
from scrapy import signals


class RedisPublisherPipeline(object):
    """A pipeline that uses a Redis server as a data buffering"""

    @classmethod
    def from_crawler(cls, crawler):
        """Create a new instance and pass it Redis' url and namespace"""

        # Get redis URL
        redis_url = crawler.settings.get('REDIS_PIPELINE_URL', None)

        # If doesn't exist, disable
        if not redis_url:
            raise NotConfigured

        redis_channel = crawler.settings.get(
            'REDIS_PIPELINE_CHANNEL', 'PORTS_CHANNEL'
        )

        return cls(crawler, redis_url, redis_channel)

    def __init__(self, crawler, redis_url, redis_channel):
        """Store configuration, open connection and register callback"""

        # Store the url and the namespace for future reference
        self.redis_url = redis_url
        self.redis_channel = redis_channel

        # Report connection error only once
        self.report_connection_error = True

        # Parse redis URL and try to initialize a connection
        args = RedisPublisherPipeline.parse_redis_url(redis_url)
        self.connection = txredisapi.lazyConnectionPool(connectTimeout=5,
                                                        replyTimeout=5,
                                                        **args)

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        """Publish item into buffer."""

        logger = spider.logger

        try:
            print("Publishing", "****"*20)
            yield self.connection.publish(self.redis_channel, 'Hello World')
        except txredisapi.ConnectionError:
            if self.report_connection_error:
                logger.error("Can't connect to Redis: %s" % self.redis_url)
                self.report_connection_error = False

        defer.returnValue(item)

    @staticmethod
    def parse_redis_url(redis_url):
        """
        Parses redis url and prepares arguments for
        txredisapi.lazyConnectionPool()
        """

        params = dj_redis_url.parse(redis_url)

        conn_kwargs = {}
        conn_kwargs['host'] = params['HOST']
        conn_kwargs['password'] = params['PASSWORD']
        conn_kwargs['dbid'] = params['DB']
        conn_kwargs['port'] = params['PORT']

        # Remove items with empty values
        conn_kwargs = dict((k, v) for k, v in conn_kwargs.items() if v)

        return conn_kwargs
