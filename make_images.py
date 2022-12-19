
import json
import logging
import random
from string2img import *
from libfunctions import *

builder = make_spark_builder()

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(spark.sparkContext)

# define s3 bicket and folder names where the data files are 
s3BucketName = 'synthea-output'
s3FolderName =  'Bundles' # 'dupes'

# s3_client = boto3.client('s3')
# response = s3_client.upload_file("/work/delta/p360_pov.db/patients/", s3BucketName, "output/delta/patients")

# exit(0)

# make available a boto3 s3 resorce
s3 = boto3.resource('s3')

# define resources dataframes
data = []
schema = None

# makle schema for dataframes
patientSchema,encounterSchema,conditionSchema= makeSchemas()

# get a list of all the data files for looping 
fileList = list_s3_files_using_resource(s3BucketName, s3FolderName, today_only=True)

i=0 # counter for keeping track of how many images are created

# loop over the files in the s3 bucket
for fileName in fileList:

    # get specific object (JSON files)
    content_object = s3.Object(s3BucketName, fileName)

    # Load JSON body and convert to JSON object
    file_content = content_object.get()['Body'].read().decode('utf-8')

    # load JSON into a schema to by used by the FHIR library
    schema = json.loads(file_content)

    # ingest the JSON schema into a FHIR resource
    resources = ingestBundle(schema)

    # capture patient object
    onePatient = Patient.parse_obj(resources[0])
    # capture patient ID
    onePatientID = onePatient.id

    # collect and log some stats
    NumberOfResourcesInBundle = len(resources)
    print(fileName, onePatientID, i, NumberOfResourcesInBundle)

    # increase counter and break depending on setting, useful for debugging
    i+=1
    if (i > 33): 
        break

    # if NumberOfResourcesInBundle > 0 and NumberOfResourcesInBundle <= 9999:
    #     continue
    
    patientData= []
    encounterData= []
    condtitionData = []

    patient_img,encounter_img, condition_img=None,None,None
    patient_lst,encounter_lst, condition_lst=[],[],[]
    
    # this loop goes over FHIR resources in the bundle
    for j in range(len(resources)):
        
        # catpture a resource
        currentResource = resources[j]

        # check if a patient resource
        if resources[j].__class__.__name__ == 'Patient':
        
            # logging.info('adding patient info')

            # this is to generate noise used when testing matching
            rand_str= random_strings(3)
            
            # family_name_ovrd =''

            # if (onePatientID=='a9959419-fd3e-1ed5-cf4c-2887b6db1f39'):
            #     family_name_ovrd = 'smith'

            # get patient attributes that match the patient schema             
            resource_data = [onePatientID, \
                            onePatient.identifier[1].value, \
                            onePatient.name[0].family, \
                            onePatient.name[0].given[0], \
                            onePatient.birthDate, \
                            onePatient.gender, \
                            onePatient.extension[0].extension[1].valueString, \
                            onePatient.extension[1].extension[1].valueString, \
                            onePatient.deceasedBoolean, \
                            onePatient.address[0].line[0] + random_strings(2), \
                            onePatient.address[0].city, \
                            onePatient.address[0].state, \
                            onePatient.address[0].postalCode, \
                            onePatient.address[0].country]

            # Add resource to list
            patientData.append(resource_data)
            
            # create a patient data frame
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
            # create encounter data frame
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
    
    # convert data frame to a list to be passed to make an image of the patient record
    patient_lst = df2list(newPatient_df.select('PatientUID','NameFamily', \
        'NameGiven','Gender', 'city','state','postalCode'), onePatientID)

    # conver prepared patient list to an image
    patient_img=to_image(patient_lst, get_rand_mask())
    # patient_img.save('./imgs/patient_test' + onePatientID + '.png')

    encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode').orderBy(['classCode']), onePatientID, random.randint(0, 7))
    # encounter_lst = df2list(newEncounter_df.select('PatientUID','classCode'), onePatientID, 1)
    encounter_img=to_image(encounter_lst,  get_rand_mask())
    # encounter_img.save('./imgs/encounter_test' + onePatientID + '.png')

    condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode').orderBy(['conditionCode']), onePatientID, random.randint(0, 5))
    # condition_lst = df2list(newCondition_df.select('PatientUID','conditionCode'), onePatientID, 2)
    condition_img=to_image(condition_lst, get_rand_mask())
    # condition_img.save('./imgs/condition_test' + onePatientID + '.png')

    # save all images to a grid and store as PNG file
    pil_grid([patient_img,encounter_img, condition_img], 3).save('./imgs/tst375_' + onePatientID + '.png')

    patientData.clear()
    encounterData.clear()
    condtitionData.clear()
    patient_img.close()
    encounter_img.close()
    condition_img.close()

# patient_df.show(truncate=True)
# encounter_df.show(truncate=True)
# condition_df.show(truncate=True)
