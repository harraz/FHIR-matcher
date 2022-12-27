import json
import logging
from string2img import *
from libfunctions import *

builder = make_spark_builder()

spark = configure_spark_with_delta_pip(builder, extra_packages=["org.apache.spark:spark-hadoop-cloud_2.12:3.3.0"]).getOrCreate()
print(spark.sparkContext)

sc =  spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.fast.upload.buffer", "bytebuffer")

# sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
# sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

s3 = boto3.resource('s3')

content_object = s3.Object('synthea-output', 'Bundles/Ana_María762_Teresa94_Arevalo970_a9855237-bdde-707e-cf2e-6de590b79d1d.json')
file_content = content_object.get()['Body'].read().decode('utf-8')

# from IPython.display import JSON
# schema = json.loads(file_content)
# schema = json.load(open('s3://synthea-output/AWSHealthLakeData/practitionerInformation1661305219780.json'))
# JSON(schema)
# print(schema)

data = []
schema = None

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
s3FolderName =  'dupes' # 'dupes' Bundles

fileList = list_s3_files_using_resource(s3BucketName, s3FolderName)

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
    if (i > 9999):
        break

    # if NumberOfResourcesInBundle > 0 and NumberOfResourcesInBundle <= 9999:
    #     continue
    
    patientData= []
    encounterData= []
    condtitionData = []

    for j in range(len(resources)):
        
        currentResource = resources[j]

        if resources[j].__class__.__name__ == 'Patient':
        
            # logging.info('adding patient info')

            rand_str= random_strings(3)

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

patient_df.show(truncate=True)
encounter_df.show(truncate=True)
condition_df.show(truncate=True)


# spark.sql("create database if not exists p360_pov")
# spark.sql("use p360_pov")

# .option("path", "/root/work/delta") \

patient_df.write.format('delta').mode('overwrite').save('s3a://synthea-output/output/delta/patient')
encounter_df.write.format('delta').mode('overwrite').save('s3a://synthea-output/output/delta/encounters')
condition_df.write.format('delta').mode('overwrite').save('s3a://synthea-output/output/delta/conditions')

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