#-----------------------------------------------------------------------------------------------------------------
# Program: xml_pull.py
# Created Date: 06/13/2023
# Created By: 
# Modified Date: 
# Release Version: 1.0
#-----------------------------------------------------------------------------------------------------------------
from pipeline.spark.loaders.spark_loader import SparkLoader
from datetime import datetime
import requests
import base64
import json
import xml.etree.ElementTree as ET
import boto3
import sys
import time


class DatasourceApiXmlPull(SparkLoader):
    """
    Class provides possibilities to load data from custom source.
    """

    def load(self):
        """
        The only method that needs to be implemented.
        :return: Spark DataFrame
        """
        self.logger.info("Running Ienvision Person Xml Pull loader")

        secrets_client = boto3.client('secretsmanager')
        src_secret = self.configuration['input'].get('src_secret')
        secret_response = secrets_client.get_secret_value(
            SecretId=src_secret
        )
        db_credentials = json.loads(secret_response['SecretString'])

        clientURL = db_credentials['clientURL']
        clientId = db_credentials['clientId']
        clientSecret = db_credentials['clientSecret']

        self.logger.info(f"clientURL is {clientURL}")
        self.logger.info("clientId is configured")
        self.logger.info("clientSecret is configured")

        S3_BUCKET = self.configuration['input'].get('S3_BUCKET')
        s3_file_name = self.configuration['input'].get('s3_file_name')
        api_url = self.configuration['input'].get('api_url')
        last_run_date = self.configuration['input'].get('last_run_date')
        self.logger.info(f"Source System {S3_BUCKET} {s3_file_name} {api_url} {last_run_date}")

        def getOAuth2ClientCredentialsflow():
            uri = clientURL + "/oauth2/token"
            authHeader = clientId + ":" + clientSecret
            authHeaderbytes = authHeader.encode('utf-8')

            authHeaderEncoded = base64.b64encode(authHeaderbytes)
            authHeaderVal = b"Basic " + authHeaderEncoded

            date = datetime.now()
            dateHeaderValue = date.strftime('%a, %d %b %Y %X %ZGMT')
            headers = {
                'Authorization': authHeaderVal,
                'Date': dateHeaderValue,
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            requestBody = {'grant_type': 'client_credentials'}

            Response = requests.post(url=uri, headers=headers, data=requestBody)
            self.logger.info(Response)

            PostRequest = json.loads(Response.text)
            return PostRequest

        OAuthToken = getOAuth2ClientCredentialsflow()
        self.logger.info(OAuthToken)
        token_type = OAuthToken['token_type']
        self.logger.info(token_type)

        if token_type == 'Bearer':
            uri = clientURL + api_url

            criteria = "{" + '"criteria"' + ':[{"lastModifiedDate": {"GREATER_THAN_OR_EQUALS":"' + last_run_date + '"' + "}}]}"
            self.logger.info(criteria)

            self.logger.info(uri)
            access_token = OAuthToken['access_token']
            authHeaderValue = "Bearer " + access_token
            self.logger.info("Bearer token generated")

            startdate = datetime.now()
            dateHeaderValue = startdate.strftime('%a, %d %b %Y %X %ZGMT')
            headers = {
                'Authorization': authHeaderValue,
                'Date': dateHeaderValue,
                'Accept': 'application/xml',
                'Content-Type': 'application/xml'
            }

            Response = requests.post(url=uri, data=criteria, headers=headers)
            self.logger.info(Response)

            PostRequest = json.dumps(Response.text)
            Data = json.loads(PostRequest)
            Data = Data.encode('utf-8')

            s3 = boto3.client('s3')
            s3.put_object(
                Bucket=S3_BUCKET,
                Body=Data,
                Key='folder/vision/' + s3_file_name + '_1.xml',
                ACL='bucket-owner-full-control'
            )

            root = ET.fromstring(Data)

            for i in root.iter('data'):
                numpages = i.attrib['numPages']
                resultSize = i.attrib['resultSize']

            self.logger.info(numpages)
            self.logger.info(resultSize)

            if numpages > '1':
                page = 2
                self.logger.info(page)

                while page <= int(numpages):
                    timenow = datetime.now()
                    timediff = (timenow - startdate)
                    self.logger.info(timediff)
                    time_diff_inseconds = timediff.total_seconds()
                    self.logger.info(time_diff_inseconds)

                    if time_diff_inseconds >= 800:
                        startdate = datetime.now()
                        dateHeaderValue = startdate.strftime('%a, %d %b %Y %X %ZGMT')
                        self.logger.info("Refresh Token")
                        OAuthToken = getOAuth2ClientCredentialsflow()
                        access_token = OAuthToken['access_token']
                        authHeaderValue = "Bearer " + access_token
                        self.logger.info("Bearer token refreshed")

                        headers = {
                            'Authorization': authHeaderValue,
                            'Date': dateHeaderValue,
                            'Accept': 'application/xml',
                            'Content-Type': 'application/xml'
                        }

                        URL = clientURL + api_url + "?pageNum=" + str(page)
                        Response = requests.post(url=URL, headers=headers, data=criteria)
                        self.logger.info(Response)

                        if Response.status_code == 429 or Response.status_code == 503 or not Response.text.strip():
                            self.logger.warning('Status code: %s', Response.status_code)
                            time.sleep(65)
                            date = datetime.now()
                            dateHeaderValue = date.strftime('%a, %d %b %Y %X %ZGMT')
                            headers = {
                                'Authorization': authHeaderValue,
                                'Date': dateHeaderValue,
                                'Accept': 'application/xml',
                                'Content-Type': 'application/xml'
                            }
                            Response = requests.post(url=URL, headers=headers, data=criteria)
                            self.logger.info(Response)

                        PostRequest = json.dumps(Response.text)
                        Data = json.loads(PostRequest)
                        Data = Data.encode('utf-8')
                        s3.put_object(
                            Bucket=S3_BUCKET,
                            Body=Data,
                            Key='folder/vision/' + s3_file_name + '_' + str(page) + '.xml',
                            ACL='bucket-owner-full-control'
                        )
                        page = page + 1
                        self.logger.info(page)

                    else:
                        date = datetime.now()
                        dateHeaderValue = date.strftime('%a, %d %b %Y %X %ZGMT')
                        headers = {
                            'Authorization': authHeaderValue,
                            'Date': dateHeaderValue,
                            'Accept': 'application/xml',
                            'Content-Type': 'application/xml'
                        }

                        URL = clientURL + api_url + "?pageNum=" + str(page)
                        Response = requests.post(url=URL, headers=headers, data=criteria)
                        self.logger.info(Response)

                        if Response.status_code == 429 or Response.status_code == 503 or not Response.text.strip():
                            self.logger.warning('Status code: %s', Response.status_code)
                            time.sleep(65)
                            date = datetime.now()
                            dateHeaderValue = date.strftime('%a, %d %b %Y %X %ZGMT')
                            headers = {
                                'Authorization': authHeaderValue,
                                'Date': dateHeaderValue,
                                'Accept': 'application/xml',
                                'Content-Type': 'application/xml'
                            }
                            URL = clientURL + api_url + "?pageNum=" + str(page)
                            Response = requests.post(url=URL, headers=headers, data=criteria)
                            self.logger.info(Response)

                        PostRequest = json.dumps(Response.text)
                        Data = json.loads(PostRequest)
                        Data = Data.encode('utf-8')
                        s3.put_object(
                            Bucket=S3_BUCKET,
                            Body=Data,
                            Key='folder/vision/' + s3_file_name + '_' + str(page) + '.xml',
                            ACL='bucket-owner-full-control'
                        )
                        page = page + 1
                        self.logger.info(page)
