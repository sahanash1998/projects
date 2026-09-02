

# pipeline base loader will be imported when airflow dag is created
from pipeline.spark.loaders.spark_loader import SparkLoader
import boto3
from pyspark.sql.types import StructType, StructField, StringType
from .helper import send_email, move_files_s3

class ArchiveLoader(SparkLoader):
    """
    Class to load veeva crm data into archive location.
    """

    def load(self):
        """
        The only method that needs to be implemented.
        :return: Spark DataFrame
        """
        self.logger.info("Running Archive Loader")
        # you can read your custom properties from the yaml configuration file
        # custom_val = self.configuration['input'].get('loader_custom_property')
        # self.self.logger.info(f"Custom loader property: {custom_val}")

        # Initialize AWS Secrets Manager client
        secrets_client = boto3.client('secretsmanager')
    
        stage_s3_bucket = archive_s3_bucket = stage_path = archive_path = None
        has_errors = False
        src_system = None
        
        try:
           
            src_system = self.configuration['input'].get('src_system')
            stage_s3_bucket = self.configuration['input'].get('stage_s3_bucket')
            archive_s3_bucket = self.configuration['input'].get('archive_s3_bucket')
            stage_path = self.configuration['input'].get('stage_path')
            archive_path = self.configuration['input'].get('archive_path')
    
            self.logger.info(f"Moving data from Stage bucket: {stage_s3_bucket}/{stage_path} to archive bucket: {archive_s3_bucket}/{archive_path}")
            move_files_s3(self, stage_s3_bucket, archive_s3_bucket, stage_path, archive_path)
        except Exception as e:
            subject = f"Data pipeline error in {src_system}-{self.configuration['input'].get('format')}"
            error_msg = str(e)
            self.logger.info("Sending email notification")
            send_email(self, subject, error_msg, stage_path, archive_path)
            has_errors = True
            raise e
        finally:
            if not has_errors:
                #email success message
                subject = f"Archive loader successful for {src_system}-{self.configuration['input'].get('format')}"
                msg = 'Archive loader completed successfully'
                self.logger.info("Sending job success email notification")
                send_email(self, subject, msg, stage_s3_bucket, archive_s3_bucket) 
    
    
