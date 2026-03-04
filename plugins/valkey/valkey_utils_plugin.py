from airflow.hooks.base import BaseHook
import redis


class ValkeyConnector:

    _client = None

    @staticmethod
    def get_client():

        if ValkeyConnector._client is None:

            conn = BaseHook.get_connection("valkey_default")

            ValkeyConnector._client = redis.Redis(
                host=conn.host,
                port=conn.port,
                password=conn.password,
                decode_responses=True
            )

        return ValkeyConnector._client


    @staticmethod
    def push(key, value):
        """
        Store temporary data
        """

        client = ValkeyConnector.get_client()

        client.set(key, value)


    @staticmethod
    def pull(key):
        """
        Retrieve stored data
        """

        client = ValkeyConnector.get_client()

        return client.get(key)


    @staticmethod
    def delete(key):
        """
        Delete stored data
        """

        client = ValkeyConnector.get_client()

        client.delete(key)