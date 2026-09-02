#-----------------------------------------------------------------------------------------------------------------
# Program: xml_to_csv.py
# Created Date: 06/13/2023
# Created By: 
# Modified Date:03/12/2024
# Release Version: 1.0
#-----------------------------------------------------------------------------------------------------------------
from pipeline.spark.loaders.spark_loader import SparkLoader
import json
import xml.etree.ElementTree as ET
import boto3
import csv
import re
import sys

#command line arguments

class XmlToCsv(SparkLoader):
    """
    Class provides possibilities to load data from custom source.
    """

    def load(self):

      self.logger.info("Running t Xml To Csv loader")
      S3_BUCKET = self.configuration['input'].get('S3_BUCKET')
      self.logger.info(f"Source System {S3_BUCKET}")

      def clean_text(value):
          if value is None:
              return None
          if isinstance(value, str):
              return value.strip().replace('\r', ' ').replace('\n', ' ')
          return value

      ###Defining the labels and their attributes for the data to be fetched from XML file
      labels={
        'pubs_dvdocument':['id','lastModifiedDate','creationDate','title','documentNumber','shortTitle','underOSTICReview','osticNumber','earlyViewDisabled','nonCSR','nextSteps','pubPlan','highProfileProject','targetName','authorByline','firstPublishedResults','permissionToShare','protocolRequired','isLateBreaker','note'],
        'otherProducts':['id','scientificName','code'],
        'studies':['studyNumber','title','acronym'],
        'dvDocumentPersons':['firstName','lastName'],
        'plan':['id'],
        'financialinfo':['id'],
        'citation':['id','citationTitle','citationAuthor','citationURL','publishedIn','citationcongress','location','presentationDate','presentationRefNum','citationJournal','citationDateIssue','citationVolume','citationIssue','citationPages','pubMedId','doi','formattedCitation','scopusCount']
      }

      Child_labels={
        'person':['emailAddress'],
        'primaryProduct':['scientificName','code'],
        'milestones':['name','description','status','lastModifiedDate','milestoneDate','completedDate','baselineDate','completed','cancelled','manuallyAdded','skipped'],
        'activities':['status','activityType','totalDays','startDate','dueDate','endDate','skipped','name','description'],
        'payments':['paidorDueDate','description','paidOrDueAmount','paymentStatus','poNumber','invoiceNumber']
      }

      Child_1_labels={
        'timelineActivityTemplate':['name','description'],
        'activityPersons':['id','postedDate','comments'],
        'invoice':['vendorInvoiceNumber']
      }

      Child_2_labels={
        'person':['lastName','firstLastName','fullName','firstName','middleName','active'],
      }

      ###Defining the description attributes
      Description={
        'pubs_dvdocument':['currentRisk','enhancedContent','workflowState','region','indications','primaryCountry','functionalArea','projectType','projectCategory','therapeuticArea','gsmpRole','managedBy','disseminationType','priority','protocolStatus','assay','pathogen','biomarkers','populations','partnerCompanies','pdt','collaboratorProduct','VEST','typeOfInfection'],
        'studies':['indications','phase','functionalArea','productAreas'],
        'person':['userType'],
        'payments':['costType']
      }

      Description1={
        'activityPersons':['userType','decisionType']
      }
      
      #resources
      resource_label_map = {
        "dv_id": "id",
        "plan_id": "plan/id",
        "resources_id": "plan/resources/id",
        "resources_status": "plan/resources/status",
        "planrole_id": "plan/resources/planRole/id",
        "planrole_code": "plan/resources/planRole/planRoleCode",
        "planrole_description": "plan/resources/planRole/description",
        "planrole_active": "plan/resources/planRole/active",
        "person_id": "plan/resources/person/id",
        "person_first_name": "plan/resources/person/firstName",
        "person_last_name": "plan/resources/person/lastName",
        "person_middle_name": "plan/resources/person/middleName",
        "person_full_name": "plan/resources/person/fullName",
        "person_user_type": "plan/resources/person/userType",
        "person_emailid": "plan/resources/person/emailAddress",
        "person_active": "plan/resources/person/active"
      }

      ####parsing label and create csv
      def get_parent_data(newData,csv_file,label_name,attributes,description_atr):
          root= ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
          data=[]
          for element in root.findall('pubs_dvdocument'):
            element_data={}
            for attribute in attributes:
              text_val = getattr(element.find(attribute), 'text', None)
              element_data[attribute] = clean_text(text_val)

              if label_name != 'citation':
                if attribute=='pubPlan':
                  publication_plan = element.find('publicationPlan')
                  if publication_plan is not None:
                    element_data[attribute] = clean_text(getattr(publication_plan.find('description'),'text',None))
                  else:
                    element_data[attribute]=None

                collaborator_product = element.find('collaboratorProduct')
                element_data['collaboratorProduct_id'] = clean_text(getattr(collaborator_product,'text',None))

                for i in description_atr:
                  description_list=[]
                  for fields in element.findall(i):
                    if i in ['indications','assay','pathogen','biomarkers','populations','partnerCompanies','pdt','VEST','typeOfInfection']:
                      description_list.append(clean_text(fields.attrib.get('description')))
                      if len(description_list)>1:
                        element_data[i]='|'.join(description_list)
                      else:
                        element_data[i]=description_list[0]
                    else:
                      element_data[i]=clean_text(fields.attrib.get('description'))

                for value in element.findall('primaryProduct'):
                  element_data['scientificName']=clean_text(getattr(value.find('scientificName'),'text',None))
                  element_data['code']=clean_text(getattr(value.find('code'),'text',None))

            if label_name == 'pubs_dvdocument':
              current_version_elem = element.find('currentVersionTag')
              element_data['current_version'] = clean_text(current_version_elem.attrib.get('description')) if current_version_elem is not None else None
              element_data['other_target'] = clean_text(getattr(element.find('otherTarget'),'text',None))
              element_data['ude_target'] = clean_text(getattr(element.find('udeTarget'),'text',None))

            data.append(element_data)

          with open(csv_file,'w') as csv_file:
            if label_name=='pubs_dvdocument':
              writer=csv.DictWriter(
                csv_file,
                fieldnames=attributes+description_atr+['collaboratorProduct_id']+['scientificName']+['code']+['current_version','other_target','ude_target'],
                quoting=csv.QUOTE_ALL
              )
            elif label_name=='citation':
              writer=csv.DictWriter(csv_file,fieldnames=attributes,quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
          csv_file.close()

      def get_child_data(xml_file,csv_file,label_name,attributes,child_label,child_attr,description_atr):
          root= ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
          data=[]
          for i in root.findall('pubs_dvdocument'):
            for val in i.findall(label_name):
              for child_val in val.findall(child_label):
                value={}
                for attribute in attributes:
                  for attr in child_attr:
                    value['dv_id']=getattr(i.find('id'),'text',None)
                    value[attribute]=getattr(val.find(attribute),'text',None)
                    value[attr]=getattr(child_val.find(attr),'text',None)

                    if label_name=='dvDocumentPersons':
                      value['dvDocumentPersons_id']=getattr(val.find('id'),'text',None)
                      value['person_id']=getattr(child_val.find('id'),'text',None)
                      value['description']=getattr(val.find('planRole').find('description'),'text',None)
                      for elem in description_atr:
                        for fields in child_val.findall(elem):
                          value[elem]=fields.attrib.get('description')

                    elif label_name=='studies':
                      value['studies_id']=getattr(val.find('id'),'text',None)
                      value['primaryProduct_id']=getattr(child_val.find('id'),'text',None)
                      for elem in description_atr:
                        indication_list=[]
                        for fields in val.findall(elem):
                          if elem =='indications':
                            indication_list.append(fields.attrib.get('description'))
                            if len(indication_list)>1:
                              value[elem]=','.join(indication_list)
                            else:
                              value[elem]=indication_list[0]
                          else:
                            value[elem]=fields.attrib.get('description')

                    elif label_name=='plan':
                      value['milestones_id']=getattr(child_val.find('id'),'text',None)

                if label_name=='dvDocumentPersons':
                  author_order = getattr(val.find('sequence'),'text',None)
                  if author_order is None:
                    author_order = getattr(val.find('id'),'text',None)
                  value['author_order'] = author_order

                data.append(value)

          with open(csv_file,'w') as csv_file:
            if label_name=='dvDocumentPersons':
              writer=csv.DictWriter(
                csv_file,
                fieldnames=['dv_id']+['dvDocumentPersons_id']+attributes+['person_id']+['description']+child_attr+description_atr+['author_order'],
                quoting=csv.QUOTE_ALL
              )
            elif label_name=='studies':
              writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+['studies_id']+attributes+['primaryProduct_id']+description_atr+child_attr,quoting=csv.QUOTE_ALL)
            elif label_name=='plan':
              writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+attributes+['milestones_id']+child_attr,quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
          csv_file.close()

      def get_child1_data(newData,csv_file,label,attributes,child_label,child_attr,description_atr,child_label1,child_attr1):
          root= ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
          data=[]
          for i in root.findall('pubs_dvdocument'):
            for val in i.findall(label):
              for child_val in val.findall(child_label):
                for child_val1 in child_val.findall(child_label1):
                  value={}
                  for attribute in attributes:
                    for attr in child_attr:
                      for attribt in child_attr1:
                        if label=='plan':
                          value['dv_id']=getattr(i.find('id'),'text',None)
                          value['plan_id']=getattr(val.find('id'),'text',None)
                          value['activities_id']=getattr(child_val.find('id'),'text',None)
                          value[attr]=getattr(child_val.find(attr),'text',None)
                          value['timelineActivityTemplate_id']=getattr(child_val1.find('id'),'text',None)
                          value[attribt]=getattr(child_val1.find(attribt),'text',None)

                        elif label=='financialinfo':
                          value['dv_id']=getattr(i.find('id'),'text',None)
                          value['payments_id']=getattr(child_val.find('id'),'text',None)
                          value[attribute]=getattr(val.find(attribute),'text',None)
                          value[attr]=getattr(child_val.find(attr),'text',None)
                          value[attribt]=getattr(child_val1.find(attribt),'text',None)
                          for elem in description_atr:
                            for fields in child_val.findall(elem):
                              value[elem]=fields.attrib.get('description')
                          for item in child_val.findall('vendor'):
                            value['name']=getattr(item.find('name'),'text',None)
                            value['vendorCode']=getattr(item.find('vendorCode'),'text',None)
                          for item1 in child_val.findall('budgetYear'):
                            value['description_budgetYear']=getattr(item1.find('description'),'text',None)
                          for item2 in val.findall('budgetDefinition'):
                            value['description_budgetDefinition']=getattr(item2.find('description'),'text',None)

                  data.append(value)

          with open(csv_file,'w') as csv_file:
            if label=='plan':
              writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+['plan_id']+['activities_id']+child_attr+['timelineActivityTemplate_id']+child_attr1,quoting=csv.QUOTE_ALL)
            elif label=='financialinfo':
              writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+attributes+['payments_id']+child_attr+description_atr+child_attr1+['name']+['vendorCode']+['description_budgetYear']+['description_budgetDefinition'],quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
          csv_file.close()

      def get_child2_data(newData,csv_file,label,attributes,child_label,child_attr,description_atr1,child_label1,child_attr1,child_label2,child_attr2):
          root= ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
          data=[]
          for i in root.findall('pubs_dvdocument'):
            for val in i.findall(label):
              for child_val in val.findall(child_label):
                for child_val1 in child_val.findall(child_label1):
                  for child_val2 in child_val1.findall(child_label2):
                    value={}
                    for attribute in attributes:
                      for attr in child_attr:
                        for attribt in child_attr1:
                          for attribb in child_attr2:
                            value['dv_id']=getattr(i.find('id'),'text',None)
                            value['plan_id']=getattr(val.find('id'),'text',None)
                            value['activities_id']=getattr(child_val.find('id'),'text',None)
                            value['person_id']=getattr(child_val2.find('id'),'text',None)
                            value['description']=getattr(child_val1.find('planRole').find('description'),'text',None)
                            value[attribt]=getattr(child_val1.find(attribt),'text',None)
                            value[attribb]=getattr(child_val2.find(attribb),'text',None)
                            for elem in description_atr1:
                              for fields in child_val1.findall(elem):
                                value[elem]=fields.attrib.get('description')
                              for fields in child_val2.findall(elem):
                                value[elem]=fields.attrib.get('description')
                    data.append(value)

          with open(csv_file,'w') as csv_file:
            writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+['plan_id']+['activities_id']+child_attr1+['person_id']+['description']+child_attr2+description_atr1,quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
          csv_file.close()

      def get_data(xml_file,csv_file,label_name,attributes):
          root= ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
          data=[]
          for i in root.findall('pubs_dvdocument'):
            for val in i.findall(label_name):
              value={}
              for attribute in attributes:
                value['dv_id']=getattr(i.find('id'),'text',None)
                value[attribute]=getattr(val.find(attribute),'text',None)
              data.append(value)
          with open(csv_file,'w') as csv_file:
            if label_name=='otherProducts':
              writer=csv.DictWriter(csv_file,fieldnames=['dv_id']+attributes,quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(data)
          csv_file.close()

      #resources function
      def extract_dvdocument_resources(xml_string, csv_file_path):
            def extract_by_path(element, path):
                segments = path.split('/')
                curr = element
                for seg in segments:
                    if curr is not None:
                        curr = curr.find(seg)
                    else:
                        return None
                return curr.text if curr is not None else None

            root = ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', xml_string))
            rows = []
            for dvdoc in root.findall("pubs_dvdocument"):
                dv_id = extract_by_path(dvdoc, resource_label_map["dv_id"])
                plan_list = dvdoc.findall("plan")
                for plan in plan_list:
                    plan_id = extract_by_path(plan, resource_label_map["plan_id"].replace("plan/", ""))
                    resources_list = plan.findall("resources")
                    for resources in resources_list:
                        row = {}
                        row["dv_id"] = dv_id
                        row["plan_id"] = plan_id
                        row["resources_id"] = extract_by_path(resources, "id")
                        row["resources_status"] = extract_by_path(resources, "status")
                        planRole = resources.find("planRole")
                        row["planrole_id"] = extract_by_path(planRole, "id") if planRole is not None else None
                        row["planrole_code"] = extract_by_path(planRole, "planRoleCode") if planRole is not None else None
                        row["planrole_description"] = extract_by_path(planRole, "description") if planRole is not None else None
                        row["planrole_active"] = extract_by_path(planRole, "active") if planRole is not None else None
                        person = resources.find("person")
                        row["person_id"] = extract_by_path(person, "id") if person is not None else None
                        row["person_first_name"] = extract_by_path(person, "firstName") if person is not None else None
                        row["person_last_name"] = extract_by_path(person, "lastName") if person is not None else None
                        row["person_middle_name"] = extract_by_path(person, "middleName") if person is not None else None
                        row["person_full_name"] = extract_by_path(person, "fullName") if person is not None else None
                        row["person_user_type"] = person.find('userType').attrib.get('description', "") if person is not None and person.find('userType') is not None else None
                        row["person_emailid"] = extract_by_path(person, "emailAddress") if person is not None else None
                        row["person_active"] = extract_by_path(person, "active") if person is not None else None
                        rows.append(row)
            with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=list(resource_label_map.keys()), quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(rows)

      #versions function
      def extract_dvdocument_versions(xml_string, csv_file_path):
            root = ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', xml_string))
            rows = []

            for dvdoc in root.findall("pubs_dvdocument"):
                dv_id = dvdoc.findtext("id")

                for version in dvdoc.findall("versions"):
                    version_id = version.findtext("id")
                    version_tag_elem = version.find("versionTag")
                    version_tag_description = version_tag_elem.attrib.get("description") if version_tag_elem is not None else None
                    version_value = version.findtext("version")

                    for file_elem in version.findall("files"):
                        primary_elems = file_elem.findall("primary")
                        primary1 = primary_elems[0].text if len(primary_elems) > 0 else None
                        primary2 = primary_elems[1].text if len(primary_elems) > 1 else None

                        row = {
                            "dv_id": dv_id,
                            "version_id": version_id,
                            "version_tag_description": version_tag_description,
                            "version": version_value,
                            "file_id": file_elem.findtext("id"),
                            "last_modified_date": file_elem.findtext("lastModifiedDate"),
                            "primary1": primary1,
                            "primary2": primary2,
                            "posted_by": file_elem.findtext("postedBy"),
                            "file_name": file_elem.findtext("fileName"),
                            "full_path": file_elem.findtext("fullPath")
                        }
                        rows.append(row)

            fieldnames = [
                "dv_id",
                "version_id",
                "version_tag_description",
                "version",
                "file_id",
                "last_modified_date",
                "primary1",
                "primary2",
                "posted_by",
                "file_name",
                "full_path"
            ]

            with open(csv_file_path, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(rows)

      #Create a S3 boto resource
      s3=boto3.resource('s3')
      bucket=s3.Bucket(S3_BUCKET)
      s3_client=boto3.client('s3')
      count=0

      #fetch the count of objects/XML files in S3 bucket
      for obj in bucket.objects.filter(Prefix="work/ienvision/DataVision/iEnvision_DvDocument_"):
          count=count+1
      self.logger.info(count)

      val=1
      while(val<=count):
            for obj in bucket.objects.filter(Prefix="work/ienvision/DataVision/iEnvision_DvDocument_"):
              key = obj.key
              if(key=='work/ienvision/DataVision/iEnvision_DvDocument_'+str(val)+'.xml'):
                  body = obj.get()['Body'].read()
                  Data=body.decode('utf-8')
                  PostRequest=json.dumps(Data)
                  Data=json.loads(PostRequest)
                  newData=Data

                  for label,attributes in labels.items():
                    self.logger.info(label)
                    self.logger.info(attributes)

                    if label=='dvDocumentPersons':
                      for child_label,child_attr in Child_labels.items():
                        if child_label=='person':
                          for tag,description_atr in Description.items():
                            if tag=='person':
                              csv_file_path=str(label)+'.csv'
                              get_child_data(newData,csv_file_path,label,attributes,child_label,child_attr,description_atr)
                              s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(label)+'/'+str(label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='otherProducts':
                      csv_file_path=str(label)+'.csv'
                      get_data(newData,csv_file_path,label,attributes)
                      s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(label)+'/'+str(label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='studies':
                      for child_label,child_attr in Child_labels.items():
                        if child_label=='primaryProduct':
                          for tag,description_atr in Description.items():
                            if tag=='studies':
                              csv_file_path=str(label)+'.csv'
                              get_child_data(newData,csv_file_path,label,attributes,child_label,child_attr,description_atr)
                              s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(label)+'/'+str(label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='plan':
                      for child_label,child_attr in Child_labels.items():
                        if child_label=='milestones':
                          csv_file_path=str(child_label)+'.csv'
                          get_child_data(newData,csv_file_path,label,attributes,child_label,child_attr,None)
                          s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(child_label)+'/'+str(child_label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                        elif child_label=='activities':
                          root = ET.fromstring(re.sub(r'&#([a-zA-Z0-9]+);?', r'#', newData))
                          activities_rows = []
                          for doc in root.findall('pubs_dvdocument'):
                            dv_id = doc.findtext('id')
                            for plan in doc.findall('plan'):
                              plan_id = plan.findtext('id')
                              for act in plan.findall('activities'):
                                row = {}
                                row['dv_id'] = dv_id
                                row['plan_id'] = plan_id
                                row['activities_id'] = act.findtext('id')
                                row['status'] = act.findtext('status')
                                row['activityType'] = act.findtext('activityType')
                                row['totalDays'] = act.findtext('totalDays')
                                row['startDate'] = act.findtext('startDate')
                                row['dueDate'] = act.findtext('dueDate')
                                row['endDate'] = act.findtext('endDate')
                                row['skipped'] = act.findtext('skipped')
                                timeline_elem = act.find('timelineActivityTemplate')
                                row['timelineActivityTemplate_id'] = None
                                if timeline_elem is not None:
                                   tid = timeline_elem.findtext('id')
                                   if tid:
                                     row['timelineActivityTemplate_id'] = tid
                                row['name'] = act.findtext('name')
                                row['description'] = act.findtext('description')
                                activities_rows.append(row)

                          csv_file_path = str(child_label) + '.csv'
                          fieldnames = [
                            'dv_id', 'plan_id', 'activities_id','status', 'activityType', 'totalDays',
                            'startDate', 'dueDate', 'endDate',
                            'skipped', 'timelineActivityTemplate_id','name', 'description'
                          ]
                          with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
                            writer.writeheader()
                            writer.writerows(activities_rows)

                          s3_client.upload_file(
                            csv_file_path,
                            S3_BUCKET,
                            'stage_data/DataVision/' + str(child_label) + '/' + str(child_label) + '_' + str(val) + '.csv',
                            ExtraArgs={'ACL': 'bucket-owner-full-control'}
                          )

                          for child_label1,child_attr1 in Child_1_labels.items():
                            if child_label1=='activityPersons':
                              for child_label2,child_attr2 in Child_2_labels.items():
                                if child_label2=='person' or child_label2=='planRole':
                                  for tag1,description_atr1 in Description1.items():
                                    if tag1=='activityPersons':
                                      csv_file_path=str(child_label1)+'.csv'
                                      get_child2_data(newData,csv_file_path,label,attributes,child_label,child_attr,description_atr1,child_label1,child_attr1,child_label2,child_attr2)
                                      s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(child_label1)+'/'+str(child_label1)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='financialinfo':
                      for child_label,child_attr in Child_labels.items():
                        if child_label=='payments':
                          for child_label1,child_attr1 in Child_1_labels.items():
                            if child_label1=='invoice':
                              for tag,description_atr in Description.items():
                                if tag=='payments':
                                  csv_file_path=str(child_label)+'.csv'
                                  get_child1_data(newData,csv_file_path,label,attributes,child_label,child_attr,description_atr,child_label1,child_attr1)
                                  s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(child_label)+'/'+str(child_label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='pubs_dvdocument':
                      for tag,description_atr in Description.items():
                        if tag=='pubs_dvdocument':
                          csv_file_path=str(label)+'.csv'
                          get_parent_data(newData,csv_file_path,label,attributes,description_atr)
                          s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(label)+'/'+str(label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                    elif label=='citation':
                      csv_file_path=str(label)+'.csv'
                      get_parent_data(newData,csv_file_path,label,attributes,None)
                      s3_client.upload_file(csv_file_path,S3_BUCKET,'stage_data/DataVision/'+str(label)+'/'+str(label)+'_'+str(val)+'.csv',ExtraArgs={'ACL': 'bucket-owner-full-control'})

                  #resources
                  csv_file_path = "dvdocument_resources.csv"
                  extract_dvdocument_resources(newData, csv_file_path)
                  s3_client.upload_file(
                    csv_file_path,
                    S3_BUCKET,
                    f'stage_data/DataVision/resources/dvdocument_resources_{val}.csv',
                    ExtraArgs={'ACL': 'bucket-owner-full-control'}
                  )

                  #versions
                  csv_file_path = "dvdocument_versions.csv"
                  extract_dvdocument_versions(newData, csv_file_path)
                  s3_client.upload_file(
                    csv_file_path,
                    S3_BUCKET,
                    f'stage_data/DataVision/dvdocument_versions/dvdocument_versions_{val}.csv',
                    ExtraArgs={'ACL': 'bucket-owner-full-control'}
                  )

            val=val+1
