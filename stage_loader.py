
# to install custom libraries for Spark like the one above,
# use the custom requirements file in the deploy folder
from pipeline.spark.loaders.spark_loader import SparkLoader
from dateutil import parser
import boto3
import json
from datetime import datetime
from pyspark.sql.functions import lit
from .helper import send_email

class StageLoader(SparkLoader):
    """
    Class provides possibilities to load data from custom source.
    """

    def load(self):
        """
        The only method that needs to be implemented.
        :return: Spark DataFrame
        """
        self.logger.info("Running the custom loader")
        # you can  read your custom properties from the configuration file
        # custom_val = self.configuration['input'].get('loader_custom_property')
        # self.self.logger.info(f"Custom loader property: {custom_val}")

        # Initialize AWS Secrets Manager client
        secrets_client = boto3.client('secretsmanager')
    
        # Get Oracle credentials from Secrets Manager
        username = password = dsn = jdbc_url = None
        src_id = src_system = None
        catalog_name = schema_name = None
        stage_volume_path = None
        try:
            src_id = self.configuration['input'].get('src_id')
            src_system = self.configuration['input'].get('src_system')
            self.logger.info(f"Source System {src_system}  {src_id}")
            src_secret = self.configuration['input'].get('src_secret')
            secret_response = secrets_client.get_secret_value(
                SecretId= src_secret # Seceret name can also be used instead of ARN
            )
            db_credentials = json.loads(secret_response['SecretString'])
            # Extract credentials
            username = db_credentials['jdbc_user']
            password = db_credentials['jdbc_password']
            jdbc_url = db_credentials['dsn']
            self.logger.info(f"Source DB is {jdbc_url}")
            self.logger.info(f" Source ID {src_id}")
            catalog_name = self.configuration['input'].get('catalog_name')
            schema_name = catalog_name
            self.logger.info(f"Schema, Catalog name is {schema_name} {catalog_name}")
            stage_volume_path = self.configuration['input'].get('stage_volume_path')
            self.logger.info(f"Stage volume path: {stage_volume_path}")
            self.logger.info(f"Stage src_system: {src_system}")
        except Exception as e:
            raise Exception(f"Error retrieving secret: {str(e)}")
        
        has_errors = False
        try:
            # Get Source system table metadata
            meta_data_tables_sql = f"""
                select distinct a.src_id,
                                a.src_nm, 
                                a.objct_id, 
                                a.objct_nm,
                                a.schema_nm, 
                                a.run_flag, 
                                b.src_view_nm, 
                                b.trgt_objct_nm, 
                                b.from_clause, 
                                b.where_clause,
                                b.last_run_status, 
                                b.last_run_date,
                                split_part(b.trgt_objct_nm, '.', 2) as target_table
                                from {catalog_name}.gmadw_integration_schema.src_dtl a 
                                left join {catalog_name}.gmadw_integration_schema.src_map_dtl b on a.objct_id = b.objct_id and a.src_id = b.src_id
                                where a.run_flag='Y' and a.src_id = {src_id} order by objct_id""".strip()

            df_src_tables = self.spark_session.sql(meta_data_tables_sql).toPandas()
            for index, src_table in df_src_tables.iterrows():
                src_id = src_table['src_id']
                src_name = src_table['src_nm']
                object_id = src_table['objct_id']
                object_name = src_table['objct_nm']
                self.logger.info(f"Data Ingestion for the object {src_name}-{object_name} is started. Object ID is {object_id}.")
                from_clause = src_table['from_clause']
                last_run_date = src_table['last_run_date']
                where_clause1 = src_table['where_clause']
                stage_table = src_table['trgt_objct_nm']
                # Extract table name
                stage_table = stage_table.split('.')[-1] 
                self.logger.info(f"stage_table =  {stage_table}")

                if where_clause1 is not None:
                    where_clause = where_clause1.replace("last_run_date", "'" + str(last_run_date) + "'")
                else:
                    where_clause = ''

                meta_data_columns_sql = f"""
                    select objct_id,src_col_nm,trgt_col_nm 
                    FROM {catalog_name}.gmadw_integration_schema.trgt_map_dtl 
                    where src_id='{src_id}' 
                    and objct_id='{object_id}' 
                    and col_flag='Y' 
                    order by col_seq
                """.strip()

                # Form the source_sql query
                df_columns = self.spark_session.sql(meta_data_columns_sql).toPandas()
                col_list = df_columns.groupby('objct_id')['src_col_nm'].agg(', '.join)
                s_cols = col_list.to_list()
                source_sql = "select " + s_cols[0] + ' ' + from_clause + ' ' + where_clause
                self.logger.info(f"Source SQL is {source_sql}")
                try:
                    # Read data from source oracle table
                    df = self.spark_session.read \
                        .format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("query", source_sql) \
                        .option("user", username) \
                        .option("password", password) \
                        .option("driver", "oracle.jdbc.OracleDriver") \
                        .option("fetchsize","2000") \
                        .load()

                    schema_str = df.schema.simpleString()
                    self.logger.info(f"DataFrame schema: {schema_str}")
                    self.logger.info(f"DataFrame count: {df.count()}")
                    
                    # Stage volume path
                    stage_table_path = f"{stage_volume_path}/{src_system}/{stage_table}"
                    # Write the dataframe to Stage location (replace current data)
                    df.write.mode("overwrite").format("avro").save(stage_table_path)
                    self.logger.info(f"Stored source data into stage table at: {stage_table_path}")
                except Exception as e:
                    error_msg = f"Error loading {stage_table} to stage volume: {str(e)}"
                    self.logger.error(error_msg)
                    has_errors = True
                    subject = f"Data pipeline error in {src_system}-{self.configuration['input'].get('format')}"
                    self.logger.info("Sending job failure email notification")
                    send_email(self, subject, error_msg, src_name, stage_table_path)

        # failsafe catch all 
        except Exception as e:
            self.logger.error(str(e))
            has_errors = True
        
        finally:
            if has_errors:
                raise Exception("One or more errors occurred during loading data to stage volume. Please check logs.")
            else: #email success message
                subject = f"Stage loader successful for {src_system}-{self.configuration['input'].get('format')}"
                msg = 'Stage loader completed successfully'
                self.logger.info("Sending job success email notification")
                send_email(self, subject, msg, src_system, stage_volume_path) 
