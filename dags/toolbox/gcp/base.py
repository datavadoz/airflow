import json
from google.oauth2 import service_account
from airflow.models.connection import Connection


class Base:
    def __init__(self, conn_id):
        self.conn_id = conn_id
        self.client = None

    def _get_credentials(self, scopes=None):
        connection = Connection.get_connection_from_secrets(self.conn_id)
        return service_account.Credentials.from_service_account_info(
            json.loads(connection.get_extra_dejson()['keyfile_dict']),
            scopes=scopes
        )

    def get_client(self,):
        pass
