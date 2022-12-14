
import json
import logging
import random
from string2img import *
from libfunctions import *

builder = make_spark_builder()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(spark.sparkContext)

s3BucketName = 'synthea-output'
s3FolderName =  'Bundles' # 'dupes'

# s3_client = boto3.client('s3')
# response = s3_client.upload_file("/work/delta/p360_pov.db/patients/", s3BucketName, "output/delta/patients")

# exit(0)

s3 = boto3.resource('s3')

# define resources dataframes
data = []
schema = None

patientSchema,encounterSchema,conditionSchema= makeSchemas()

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
    if (i > 16): 
        break

    # if NumberOfResourcesInBundle > 0 and NumberOfResourcesInBundle <= 9999:
    #     continue
    
    patientData= []
    encounterData= []
    condtitionData = []

    patient_img,encounter_img, condition_img=None,None,None
    patient_lst,encounter_lst, condition_lst=[],[],[]
    
    for j in range(len(resources)):
        
        currentResource = resources[j]

        if resources[j].__class__.__name__ == 'Patient':
        
            # logging.info('adding patient info')

            rand_str= random_strings(3)

            resource_data = [onePatientID, \
                            onePatient.identifier[1].value, \
                            onePatient.name[0].family + rand_str, \
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
                            onePatient.address[0].country + rand_str]
            patientData.append(resource_data)
            
            # Adding to the patient data frame DataFrame
            newPatient_df = spark.createDataFrame(patientData, patientSchema)
        
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
    
    patient_lst = df2list(newPatient_df.select('PatientUID','NameFamily', \
        'NameGiven','Gender', 'city','state','postalCode'), onePatientID)

    patient_img=to_image(patient_lst, get_rand_mask())
    # patient_img.save('./imgs/patient_test' + onePatientID + '.png')

    encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode').orderBy(['classCode']), onePatientID, random.randint(0, 5))
    # encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode'), onePatientID, 1)
    encounter_img=to_image(encounter_lst,  get_rand_mask())
    # encounter_img.save('./imgs/encounter_test' + onePatientID + '.png')

    condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode').orderBy(['conditionCode']), onePatientID, random.randint(0, 3))
    # condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode'), onePatientID, 2)
    condition_img=to_image(condition_lst, get_rand_mask())
    # condition_img.save('./imgs/condition_test' + onePatientID + '.png')

    pil_grid([patient_img,encounter_img, condition_img], 3).save('./imgs/dupe' + onePatientID + '.png')

    patientData.clear()
    encounterData.clear()
    condtitionData.clear()
    patient_img.close()
    encounter_img.close()
    condition_img.close()

# patient_df.show(truncate=True)
# encounter_df.show(truncate=True)
# condition_df.show(truncate=True)
