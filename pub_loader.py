
# to install custom libraries for Spark like the one above,
# use the custom requirements file in the deploy folder
from pipeline.spark.loaders.spark_loader import SparkLoader
from dateutil import parser
from pyspark.sql import functions as F
import re
from .helper import send_email



class PubLoader(SparkLoader):
    """
    Class to load data from stageVEEVA CRM data from Stage  to Publish catalog
    """

    def load(self):
        """
        The only method that needs to be implemented.
        :return: Spark DataFrame
        """
        self.logger.info("Running  Publish loader")
        # you can easily read your custom properties from the configuration file
        # custom_val = self.configuration['input'].get('loader_custom_property')
        # self.self.logger.info(f"Custom loader property: {custom_val}")
    
        src_id = src_system = None
        catalog_name = schema_name = None
        stage_volume_path = None
        fq_pub_table = None
        try:
            src_id = self.configuration['input'].get('src_id')
            src_system = self.configuration['input'].get('src_system')
            catalog_name = self.configuration['input'].get('catalog_name')
            schema_name = catalog_name
            self.logger.info(f"Source System {src_system}  {src_id}")
            self.logger.info(f"Schema, Catalog name is {schema_name} {catalog_name}")
            stage_volume_path = self.configuration['input'].get('stage_volume_path')
            self.logger.info(f"Stage volume path: {stage_volume_path}")
        except Exception as e:
            raise Exception(f"Error retrieving secret: {str(e)}")
        
        src_id = int(src_id)
        # 1. Get pub-stage mapping for the selected source system (e.e. EURAM Veeva CRM)
        has_errors = False
        try:
            meta_data_tables_sql = f"""
                select distinct a.src_id,
                                a.src_nm,
                                a.objct_id
                from {catalog_name}.gmadw_integration_schema.src_dtl a 
                where a.run_flag='Y' and a.src_id = {src_id} and objct_id in (2,3,4,5,6,7,8,9,10,12,13)
                order by objct_id
            """.strip()
            stage_pub_map = self.spark_session.sql(meta_data_tables_sql)
            for row in stage_pub_map.collect():
                src_id = row['src_id']
                object_id = row['objct_id']
                meta_data_columns_sql = f"""
                    SELECT src_id, objct_id,
                        stage_schema_name,
                        stage_table_name,
                        pub_schema_name,
                        pub_table_name,
                        stage_column_name,
                        pub_column_name,
                        col_seq
                    FROM {catalog_name}.gmadw_integration_schema.stage_to_publish_mapping 
                    where src_id = '{src_id}' and objct_id = '{object_id}' order by src_id,objct_id,col_seq 
                    """.strip()
                try:
                    
                    df_columns = self.spark_session.sql(meta_data_columns_sql)
                    # Update dictionary with obj_id as key and df_columns as value
                    # Load staged data into Pub table with CDC
                    stage_columns_list = df_columns.groupby('objct_id').agg(F.expr("concat_ws(', ', array_sort(collect_list(struct(col_seq, stage_column_name))).stage_column_name)").alias('stage_columns'))
                    stage_columns = [row['stage_columns'] for row in stage_columns_list.collect()][0]
                    self.logger.info(f"Stage columns {stage_columns}")
                    pub_columns_list = df_columns.groupby('objct_id').agg(F.expr("concat_ws(', ', array_sort(collect_list(struct(col_seq, pub_column_name))).pub_column_name)").alias('pub_columns'))
                    pub_columns = [row['pub_columns'] for row in pub_columns_list.collect()][0]
                    self.logger.info(f"Pub columns {pub_columns}")
                    #unique_columns_list = df_columns.groupby('objct_id').agg(F.concat_ws(', ', F.collect_list('unique_pub_column')).alias('unique_columns'))
                    #unique_columns = [row['unique_columns'] for row in unique_columns_list.collect()][0]
                    #self.logger.info(f"Unique columns {unique_columns}")
                    stage_table = df_columns.collect()[0]['stage_table_name']
                    #.ex_us_vvm_accoungmadw_veevacrm_staget
                    self.logger.info(f"Stage table {stage_table}")
                    pub_table = df_columns.collect()[0]['pub_table_name']
                    self.logger.info(f"Pub table {pub_table}")
                    stage_schema = df_columns.collect()[0]['stage_schema_name']
                    # Both stage and Pub tables are in the same underlying schema with the proposed design
                    fq_pub_table = catalog_name + '.' + df_columns.collect()[0]['pub_schema_name']  + '.' + pub_table
                    # Stage Table storage path
                    self.logger.info(f"Pub table {fq_pub_table}")
                    #stage_table_path = f"{stage_volume_path}/{stage_table}"
                    stage_table_path = f"{stage_schema}"
                    self.logger.info(f"Stage Table {stage_table_path}  Pub Table {pub_table}")
                    #Check path
                    # fs = self.spark_session._jvm.org.apache.hadoop.fs.FileSystem.get(
                    #         self.spark_session._jsc.hadoopConfiguration())
                    # path = self.spark_session._jvm.org.apache.hadoop.fs.Path(stage_table_path)
                    # path_exists = fs.exists(path)
                    # if path_exists:
                    self.logger.info(f"Stage Table {stage_table_path} exists.")
                    delete_sql = f"""delete from {fq_pub_table}""".strip()
                    
                    insert_sql = f"""insert into {fq_pub_table}({pub_columns}) 
                                    select {stage_columns} from 
                                    read_files(
                                    '{stage_table_path}',
                                   format => 'csv',
                                   quote => '"',
                                   escape => '"',
                                   multiLine => true,
                                   timestampFormat =>'yyyy-MM-dd-HH:mm',
                                   header => true)
                                """.strip()
                    # Delete existing records from Pub table
                    self.logger.info(f"Deleting existing records from pub table {fq_pub_table} using query {delete_sql}")
                    self.spark_session.sql(delete_sql)
                    # Insert new records into Pub table
                    # Replace invalid date_format dbx error - need to review
                    # to_date('3000-01-01','YYYY-MM-DD HH24:MI:SS') DATE('3000-01-01')
                    # insert_sql = re.sub(r"to_date\('3000-01-01',\s*'[^']*'\)", "DATE('3000-01-01')", insert_sql)
                    self.logger.info(f"Inserting new records into Pub table {fq_pub_table} using query {insert_sql}")
                    self.spark_session.sql(insert_sql)
                    self.logger.info(f"Data stored to Pub for {fq_pub_table}.")

                    # else:
                    #     self.logger.info(f"Stage Table {stage_table_path} does not exist for pub table {fq_pub_table}.")
                except Exception as e:
                    error_msg = f"Error loading data to Publish for table {fq_pub_table}: {str(e)}"
                    self.logger.error(error_msg)
                    has_errors = True
                    subject = f"Data pipeline error in {self.configuration['input'].get('table_name')}-{self.configuration['input'].get('format')}"
                    self.logger.info("Sending email notification")
                    send_email(self, subject, error_msg, stage_table_path, fq_pub_table)
                    continue
        except Exception as e:
            self.logger.error(str(e))
            has_errors = True
        
        finally:
            if has_errors:
                raise Exception("One or more errors occurred during loading data to Publish. Please check logs.")
            else: #email success message
                subject = f"Pub loader successful for {self.configuration['input'].get('table_name')}-{self.configuration['input'].get('format')}"
                msg = 'Pub loader completed successfully'
                self.logger.info("Sending job success email notification")
                send_email(self, subject, msg, src_system, stage_volume_path)   
    
    
