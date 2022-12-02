import json
import logging
from string2img import *
from libfunctions import *

from fhir.resources.bundle import Bundle
from fhir.resources.patient import Patient
# from fhir.resources.condition import Condition
# from fhir.resources.observation import Observation
# from fhir.resources.medicationrequest import MedicationRequest
# from fhir.resources.procedure import Procedure
# from fhir.resources.encounter import Encounter
# from fhir.resources.claim import Claim
# from fhir.resources.immunization import Immunization

import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("p360POC") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.hive.metastore.warehouse.dir", "/work/delta") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:1.1.0,"
                "org.apache.hadoop:hadoop-aws:3.2.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.180") \
    .config('spark.executor.instances', 8) \
    .config("spark.executor.memory", "8g")
    # .config("spark.sql.warehouse.dir", "/work/delta") \

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(spark.sparkContext)

# This is mandate config on spark session to use AWS S3
# spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
# spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
# spark.sparkContext.setLogLevel("DEBUG")

sc =  spark.sparkContext
# sc.addPyFile("")

# s3_client = boto3.client('s3')
# response = s3_client.upload_file("/work/delta/p360_pov.db/patients/", 'synthea-output', "output/delta/patients")

# exit(0)

logging.basicConfig(filename='unixmen.log', level=logging.DEBUG)

s3 = boto3.resource('s3')

content_object = s3.Object('synthea-output', 'Bundles/Ana_María762_Teresa94_Arevalo970_a9855237-bdde-707e-cf2e-6de590b79d1d.json')
file_content = content_object.get()['Body'].read().decode('utf-8')

# from IPython.display import JSON
schema = json.loads(file_content)
# schema = json.load(open('s3://synthea-output/AWSHealthLakeData/practitionerInformation1661305219780.json'))
# JSON(schema)
# print(schema)

def ingestBundle(schema):
    
    """
    This functions extracts all the resources in a given schema and passed back a list of resources found.
    :return: list
    :input: a schema of type json
    """

    oneBundle = Bundle.parse_obj(schema)

    # Resources

    resources = []
    if oneBundle.entry is not None:
        for entry in oneBundle.entry:
            resources.append(entry.resource)
    
    return resources

resources = ingestBundle(schema)

    # print(resources[0])
onePatient = Patient.parse_obj(resources[0])

onePatient.name[0]

# Patient demographics
onePatientID = onePatient.id

# define resources dataframes
data = []

# Creating an empty RDD to make a DataFrame
# with no data
emp_RDD = spark.sparkContext.emptyRDD()

patientSchema,encounterSchema,conditionSchema= makeSchemas()

# Creating an empty patient DataFrame
patient_df = spark.createDataFrame(data=emp_RDD, schema=patientSchema)
# Creating an empty encounter DataFrame
encounter_df = spark.createDataFrame(data=emp_RDD, schema=encounterSchema)
# Creating an empty encounter DataFrame
condition_df = spark.createDataFrame(data=emp_RDD, schema=conditionSchema)

s3BucketName = 'synthea-output'
s3FolderName =  'Bundles' # 'dupes'

fileList = list_s3_files_using_resource(s3BucketName, s3FolderName)

s3 = boto3.resource('s3')
i=0
for fileName in fileList:

    content_object = s3.Object(s3BucketName, fileName)
    file_content = content_object.get()['Body'].read().decode('utf-8')

    schema = json.loads(file_content)

    resources = ingestBundle(schema)

    onePatient = Patient.parse_obj(resources[0])
    onePatientID = onePatient.id

    NumberOfResourcesInBundle = len(resources)
    print(fileName, onePatientID, i, NumberOfResourcesInBundle)

    i+=1
    if (i > 900): 
        break

    if NumberOfResourcesInBundle > 9999:
        continue
    
    patientData= []
    encounterData= []
    condtitionData = []

    patient_img,encounter_img, condition_img=None,None,None
    patient_lst,encounter_lst, condition_lst=[],[],[]
    
    for j in range(len(resources)):
        
        currentResource = resources[j]

        if resources[j].__class__.__name__ == 'Patient':
        
            # logging.info('adding patient info')

            resource_data = [onePatientID, \
                            onePatient.identifier[1].value, \
                            onePatient.name[0].family, \
                            onePatient.name[0].given[0], \
                            onePatient.birthDate, \
                            onePatient.gender, \
                            onePatient.extension[0].extension[1].valueString, \
                            onePatient.extension[1].extension[1].valueString, \
                            onePatient.deceasedBoolean, \
                            onePatient.address[0].line[0], \
                            onePatient.address[0].city, \
                            onePatient.address[0].state, \
                            onePatient.address[0].postalCode, \
                            onePatient.address[0].country]
            patientData.append(resource_data)
            
            # Adding to the patient data frame DataFrame
            newPatient_df = spark.createDataFrame(patientData, patientSchema)
            patient_df = patient_df.union(newPatient_df)
        
        if resources[j].__class__.__name__ == 'Encounter':

            # logging.info('adding encounter')
        
            reasonCode= None
            reasonText = None
    
            if (currentResource.reasonCode != None):
                reasonCode = currentResource.reasonCode[0].coding[0].code
                reasonText = currentResource.reasonCode[0].coding[0].display
        
            resource_data= [onePatientID, currentResource.id, \
                currentResource.status, \
                currentResource.type[0].coding[0].code, \
                currentResource.type[0].coding[0].display, \
                currentResource.period.start, \
                currentResource.period.end, reasonCode, reasonText, \
                currentResource.serviceProvider.display]
            
            encounterData.append(resource_data)
            # Adding to the encounter data frame DataFrame
            newEncounter_df = spark.createDataFrame(encounterData, encounterSchema)
            encounter_df = encounter_df.union(newEncounter_df)
            
        if resources[j].__class__.__name__ == 'Condition':
            
            # logging.info('adding condition')

            resource_data = [onePatientID, \
                            currentResource.id, \
                            currentResource.encounter.reference, \
                            currentResource.clinicalStatus.coding[0].code, \
                            currentResource.verificationStatus.coding[0].code, \
                            currentResource.code.coding[0].code, \
                            currentResource.code.coding[0].display, \
                            (currentResource.onsetDateTime).date(), \
                            (currentResource.recordedDate).date()]
            condtitionData.append(resource_data)

            # Adding to the condition DataFrame
            newCondition_df = spark.createDataFrame(condtitionData, conditionSchema)
            condition_df = condition_df.union(newCondition_df)
    
    patient_lst = df2list(newPatient_df.select('PatientUID','NameFamily', \
        'NameGiven','Gender', 'city','state','postalCode'), onePatientID)

    patient_img=to_image(patient_lst, 'patient_mask.png')
    # patient_img.save('./imgs/patient_test' + onePatientID + '.png')

    encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode').orderBy(['classCode']), onePatientID)
    # encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode'), onePatientID, 1)
    encounter_img=to_image(encounter_lst,  'encounter_mask.png')
    # encounter_img.save('./imgs/encounter_test' + onePatientID + '.png')

    condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode').orderBy(['conditionCode']), onePatientID)
    # condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode'), onePatientID, 2)
    condition_img=to_image(condition_lst, 'condition_mask.png')
    # condition_img.save('./imgs/condition_test' + onePatientID + '.png')

    pil_grid([patient_img,encounter_img, condition_img], 3).save('./imgs/grid4_' + onePatientID + '.png')

    patient_img.close()
    encounter_img.close()
    condition_img.close()

patient_df.show(truncate=True)
encounter_df.show(truncate=True)
condition_df.show(truncate=True)


# spark.sql("create database if not exists p360_pov")
# spark.sql("use p360_pov")

# .option("path", "/root/work/delta") \

# patient_df.write \
#     .format("delta") \
#         .option("overwriteSchema", "true") \
#     .mode("overwrite") \
#     .saveAsTable("patients")

# encounter_df.write \
#     .format("delta") \
#         .option("overwriteSchema", "true") \
#     .mode("overwrite") \
#     .saveAsTable("encounters")

# condition_df.write \
#     .format("delta") \
#     .option("overwriteSchema", "true") \
#     .mode("overwrite") \
#     .saveAsTable("conditions")

# patient_df.write.option("header",True).option("delimiter","|").csv("/work/csv/patient")
# encounter_df.write.option("header",True).option("delimiter","|").csv("/work/csv/encounter")
# condition_df.write.option("header",True).option("delimiter","|").csv("/work/csv/condition")