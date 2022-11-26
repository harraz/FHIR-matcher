import boto3
from pyspark.sql.types import StructType, StructField, StringType, DateType

def list_s3_files_using_resource(bucketName, folderName):
    """
    This functions list files from s3 bucket using s3 resource object.
    :return: list
    """
    
    fileList= []
    s3_resource = boto3.resource("s3")
    s3_bucket = s3_resource.Bucket(bucketName)
    files = s3_bucket.objects.all()
    for file in files:
        if (file.key.startswith(folderName + '/') and file.key.endswith('.json')):
            fileList.append(file.key)
    return fileList

def df2list(df, onePatientID, take='all'):
    
    if (take=='all'):
        lst = df.filter(df.PatientUID == onePatientID) \
        .distinct() \
        .rdd.flatMap(lambda x : x[1:]) \
        .collect()
    else:
        lst = df.filter(df.PatientUID == onePatientID) \
        .distinct() \
        .rdd.flatMap(lambda x : x[1:]) \
        .take(take)
    return lst

def makeSchemas():

    patientSchema = StructType([
        StructField('PatientUID', StringType(), True),
        StructField('PatientRecordNumber', StringType(), True),
        StructField('NameFamily', StringType(), True),
        StructField('NameGiven', StringType(), True),
        StructField('DoB', DateType(), True),
        StructField('Gender', StringType(), True),
        StructField('CoreEthnicity', StringType(), True),
        StructField('CoreEthnicity2', StringType(), True),
        StructField('isDead', StringType(), True),
        StructField('addressline1', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('postalCode', StringType(), True),
        StructField('country', StringType(), True)
    ])

    encounterSchema = StructType([
        StructField('PatientUID', StringType(), True),
        StructField('encounterID', StringType(), True),
        StructField('status', StringType(), True),
        StructField('classCode', StringType(), True),
        StructField('classText', StringType(), True),
        StructField('periodStart', DateType(), True),
        StructField('periodEnd', DateType(), True),
        StructField('reasonCode', StringType(), True),
        StructField('reasonText', StringType(), True),
        StructField('serviceProviderText', StringType(), True)
    ])

    conditionSchema = StructType([
        StructField('PatientUID', StringType(), True),
        StructField('conditionID', StringType(), True),
        StructField('encounterID', StringType(), True),
        StructField('clinicalStatus', StringType(), True),
        StructField('verificationStatus', StringType(), True),
        StructField('conditionCode', StringType(), True),
        StructField('conditionText', StringType(), True),
        StructField('onsetDateTime', DateType(), True),
        StructField('recordedDate', DateType(), True)
    ])

    return patientSchema, encounterSchema, conditionSchema