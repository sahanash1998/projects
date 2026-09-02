from datetime import datetime
import boto3
import json
import csv
import io
from pipeline.spark.loaders.spark_loader import SparkLoader
from .helper import send_email


class JsonToCsv(SparkLoader):
    """
    Downloads the most recent reports and records JSON files from S3,
    extracts metadata and record values for configured report IDs,
    writes two CSV outputs, and uploads them back to S3.
    """

    def get_latest_s3_file(self, s3_client, bucket, path, prefix):
        """
        Return the latest JSON file key from S3 for the given prefix.
        """
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path + prefix)
        files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".json")]

        if not files:
            raise Exception(f"No JSON files found in S3 with prefix {path + prefix}")

        if len(files) == 1:
            return files[0]["Key"]

        latest_obj = max(files, key=lambda obj: obj["LastModified"])
        return latest_obj["Key"]

    def load(self):
        self.logger.info("Running Veeva Link Report/Records CSV workflow")

        stage_s3_bucket = None

        try:
            # --- Read config values ---
            input_config = self.configuration.get("input", {})
            stage_s3_bucket = input_config.get("stage_s3_bucket")
            stage_path = input_config.get("stage_path")

            if not stage_s3_bucket:
                raise Exception("Missing required config: input.stage_s3_bucket")

            if not stage_path:
                raise Exception("Missing required config: input.stage_path")

            # Get report IDs from config
            target_report_ids = input_config.get("target_report_ids")
            if not target_report_ids:
                report_id_1 = input_config.get("report_id_1")
                report_id_2 = input_config.get("report_id_2")
                report_id_3 = self.configuration['input'].get('report_id_3')
                target_report_ids = [x for x in [report_id_1, report_id_2, report_id_3] if x]

            if not target_report_ids:
                raise Exception("No report IDs provided in config")

            target_report_ids = [str(report_id) for report_id in target_report_ids]

            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            s3_client = boto3.client("s3")

            # --- Load latest reports JSON ---
            reports_prefix = "all_reports_"
            reports_key = self.get_latest_s3_file(s3_client, stage_s3_bucket, stage_path, reports_prefix)
            self.logger.info(f"Using reports JSON: s3://{stage_s3_bucket}/{reports_key}")

            reports_obj = s3_client.get_object(Bucket=stage_s3_bucket, Key=reports_key)
            reports_data = json.load(io.BytesIO(reports_obj["Body"].read())) or {}

            # --- Load latest records JSON for each report ---
            records_all = []
            for report_id in target_report_ids:
                records_prefix = f"records_{report_id}_"
                records_key = self.get_latest_s3_file(s3_client, stage_s3_bucket, stage_path, records_prefix)
                self.logger.info(f"Using records JSON for {report_id}: s3://{stage_s3_bucket}/{records_key}")

                records_obj = s3_client.get_object(Bucket=stage_s3_bucket, Key=records_key)
                records_data = json.load(io.BytesIO(records_obj["Body"].read())) or {}
                records_all.append((report_id, records_data))

            # --- Table 1: report/column metadata ---
            table1_rows = []
            for report in (reports_data.get("reports") or []):
                report = report or {}
                report_id = str(report.get("id", ""))

                if report_id in target_report_ids:
                    rep_title = report.get("title", "")

                    for col in (report.get("columns") or []):
                        col = col or {}
                        table1_rows.append([
                            report_id,
                            rep_title,
                            col.get("id", ""),
                            col.get("title", ""),
                            col.get("type", "")
                        ])

            table1_csv_buf = io.StringIO()
            table1_writer = csv.writer(table1_csv_buf)
            table1_writer.writerow(["report_id", "report_title", "column_id", "column_title", "type"])
            table1_writer.writerows(table1_rows)

            # --- Table 2: column records ---
            table2_rows = []
            for report_id, records_data in records_all:
                records_data = records_data or {}

                for record in (records_data.get("records") or []):
                    record = record or {}
                    record_id = record.get("id", "")
                    source_workflow_id = record.get("source_workflow_id", "")
                    source_workflow_title = record.get("source_workflow_title", "")
                    fields = record.get("fields") or {}

                    if not isinstance(fields, dict):
                        self.logger.warning(
                            f"Skipping record {record_id} for report {report_id}: fields is not a dict"
                        )
                        continue

                    for col_id, entries in fields.items():
                        for entry in (entries or []):
                            entry = entry or {}
                            content = entry.get("content") or {}
                            value = content.get("value", "")

                            if isinstance(value, dict):
                                value = json.dumps(value)

                            table2_rows.append([
                                report_id,
                                record_id,
                                col_id,
                                value,
                                source_workflow_id,
                                source_workflow_title
                            ])

            table2_csv_buf = io.StringIO()
            table2_writer = csv.writer(table2_csv_buf)
            table2_writer.writerow([
                "report_id",
                "record_id",
                "column_id",
                "content_value",
                "source_workflow_id",
                "source_workflow_title"
            ])
            table2_writer.writerows(table2_rows)

            # --- Upload Table 1 CSV to S3 ---
            table1_key = f"{stage_path}table1_report_columns/table1_report_columns_{timestamp_str}.csv"
            s3_client.put_object(
                Bucket=stage_s3_bucket,
                Key=table1_key,
                Body=table1_csv_buf.getvalue().encode("utf-8"),
                ContentType="text/csv",
                ACL="bucket-owner-full-control"
            )
            self.logger.info(f"Uploaded Table 1 CSV to s3://{stage_s3_bucket}/{table1_key}")

            # --- Upload Table 2 CSV to S3 ---
            table2_key = f"{stage_path}table2_column_records/table2_column_records_{timestamp_str}.csv"
            s3_client.put_object(
                Bucket=stage_s3_bucket,
                Key=table2_key,
                Body=table2_csv_buf.getvalue().encode("utf-8"),
                ContentType="text/csv",
                ACL="bucket-owner-full-control"
            )
            self.logger.info(f"Uploaded Table 2 CSV to s3://{stage_s3_bucket}/{table2_key}")

        except Exception as e:
            self.logger.error(f"Error in VeevaLinkDataPull: {str(e)}")
            send_email(self, "VeevaLink Extraction Error", str(e), "", stage_s3_bucket)
        else:
            send_email(self, "VeevaLink Extraction Success", "Extraction and upload completed.", "", stage_s3_bucket)
