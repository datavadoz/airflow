import json
import os

from google.cloud import bigquery
from google.cloud.bigquery.external_config import ExternalSourceFormat

from toolbox.config import get_schema_folder, get_sql_folder
from .base import Base


class GSheetTable:
    def __init__(
            self,
            sheet_id: str,
            tab_name: str,
            schema_name: str
    ):
        self.sheet_id = sheet_id
        self.tab_name = tab_name
        self.schema_name = schema_name
        self.url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'

    def get_schema(self) -> list[dict]:
        schema_folder = get_schema_folder()
        schema_path = os.path.join(schema_folder, self.schema_name)

        with open(schema_path, 'r') as f:
            schema_str = f.read()
        return json.loads(schema_str)

    def _get_bq_external_config(self) -> bigquery.ExternalConfig:
        ext_config = bigquery.ExternalConfig(ExternalSourceFormat.GOOGLE_SHEETS)
        ext_config.source_uris = [self.url]
        ext_config.google_sheets_options.skip_leading_rows = 1
        ext_config.google_sheets_options.range = self.tab_name
        return ext_config

    def _get_bq_schema(self) -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(
                field.get('name'),
                field.get('type'),
            )
            for field in self.get_schema()
        ]

    def get_bq_table(
            self,
            full_table_id: str
    ) -> bigquery.Table:
        schema = self._get_bq_schema()
        external_config = self._get_bq_external_config()

        bq_table = bigquery.Table(full_table_id, schema=schema)
        bq_table.external_data_configuration = external_config
        return bq_table


class BigQuery(Base):
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(self, conn_id):
        super().__init__(conn_id)

    def get_client(self):
        if self.client:
            return self.client

        credentials = self._get_credentials(BigQuery.SCOPES)
        self.client = bigquery.Client(credentials=credentials)
        return self.client

    def create_bq_table_from_gsheet_table(
            self,
            gsheet_table: GSheetTable,
            full_table_id: str,
            recreate_if_exists: bool
    ) -> None:
        self.client = self.get_client()

        if recreate_if_exists:
            self.client.delete_table(full_table_id)

        bq_table = gsheet_table.get_bq_table(full_table_id)
        bq_table = self.client.create_table(bq_table)
        print(f'Created {bq_table.full_table_id}')

    def create_partitioned_table(
            self,
            full_table_id: str,
            partitioned_column: str
    ):
        sql_template_path = os.path.join(get_sql_folder(), 'sql_001.sql')
        with open(sql_template_path, 'r') as f:
            sql_template = f.read()

        sql_stmt = sql_template.format(
            full_table_id=full_table_id,
            partitioned_column=partitioned_column
        )

        self.client = self.get_client()
        query_job = self.client.query(
            sql_stmt,
            job_config=bigquery.QueryJobConfig(
                dry_run=False,
                use_query_cache=False
            )
        )

        query_job.result()
