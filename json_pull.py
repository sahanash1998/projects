from datetime import datetime
from pipeline.spark.loaders.spark_loader import SparkLoader
import boto3
import json
from .helper import send_email
import requests

class JsonPull(SparkLoader):
    """
    Class to download Veeva Link report files to S3 bucket.
    Ignores workflow, fetches reports and records per API design.
    """

    def load(self):
        self.logger.info("Running Report & Records Json Files loader")
        secrets_client = boto3.client('secretsmanager')
        try:
            src_id = self.configuration['input'].get('src_id')
            src_system = self.configuration['input'].get('src_system')
            stage_s3_bucket = self.configuration['input'].get('stage_s3_bucket')
            stage_path = self.configuration['input'].get('stage_path')
            auth_url = self.configuration['input'].get('auth_url')
            reports_url = self.configuration['input'].get('reports_url')
            report_id_1 = self.configuration['input'].get('report_id_1')
            report_id_2 = self.configuration['input'].get('report_id_2')
            report_id_3 = self.configuration['input'].get('report_id_3')
            src_secret = self.configuration['input'].get('src_secret')
            secret_response = secrets_client.get_secret_value(SecretId=src_secret)
            veevalink_api = json.loads(secret_response['SecretString'])
            veevalink_apikey = veevalink_api['token']
        except Exception as e:
            raise Exception(f"Error retrieving secret: {str(e)}")

        has_errors, subject = False, None
        try:
            s3_client = boto3.client('s3')

            # Step 1. Authenticate
            payload = json.dumps({
                "refresh": veevalink_apikey,
                "include_user_permissions": "true",
                "include_datasets": "true"
            })
            response = requests.post(auth_url, headers={'Accept': 'application/json','Content-Type': 'application/json'}, data=payload)
            response.raise_for_status()
            access_key = response.json()['access']
            auth_key = 'Bearer ' + access_key

            # Step 2. Get all reports (GET)
            headers = {'Accept': 'application/json', 'Authorization': auth_key}
            reports_resp = requests.get(reports_url, headers=headers)
            reports_resp.raise_for_status()
            reports_data = reports_resp.json()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            reports_fn = f"all_reports_{timestamp}.json"
            s3_client.put_object(
                Bucket=stage_s3_bucket,
                Key=stage_path + reports_fn,
                Body=json.dumps(reports_data, indent=2).encode("utf-8"),
                ContentType="application/json",
                ACL='bucket-owner-full-control'
            )
            self.logger.info(f"Uploaded reports JSON to s3://{stage_s3_bucket}/{stage_path}{reports_fn}")

            # Step 3. For two report IDs, POST to /v1/reports/{id}/records
            for report_id in [report_id_1, report_id_2, report_id_3]:
                records_url = f"{reports_url}/{report_id}/records"
                empty_payload = json.dumps({})
                records_resp = requests.post(records_url, headers=headers, data=empty_payload)
                records_resp.raise_for_status()
                records_data = records_resp.json()
                records_fn = f"records_{report_id}_{timestamp}.json"
                s3_client.put_object(
                    Bucket=stage_s3_bucket,
                    Key=stage_path + records_fn,
                    Body=json.dumps(records_data, indent=2).encode("utf-8"),
                    ContentType="application/json",
                    ACL='bucket-owner-full-control'
                )
                self.logger.info(f"Uploaded records JSON for report {report_id} to s3://{stage_s3_bucket}/{stage_path}{records_fn}")

        except Exception as e:
            has_errors = True
            subject = f"Error uploading Veeva Link report/records files"
            self.logger.error(f"{subject} : {str(e)}")
            self.logger.info("Sending email notification")
            send_email(self, subject, str(e), src_system, stage_s3_bucket)
        finally:
            if not has_errors:
                msg = subject = 'report and records upload completed successfully'
                self.logger.info("Sending job success email notification")
                send_email(self, subject, msg, src_system, "")
