
import boto3
from pyspark.sql.types import StructType, StructField, StringType, DateType
import matplotlib.pyplot as plt

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

def make_spark_builder():

    builder = pyspark.sql.SparkSession.builder.appName("p360POC") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.hadoop.hive.metastore.warehouse.dir", "/work/delta") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.executor.instances', 8) \
        .config("spark.executor.memory", "8g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.10.2,org.apache.hadoop:hadoop-client:2.10.2") \
        .config("spark.jars.excludes", "com.google.guava:guava")

        # .config("spark.jars.packages", 
        #             "io.delta:delta-core_2.12:1.1.0,"
        #             "org.apache.hadoop:hadoop-aws:3.2.2,"
        #             "com.amazonaws:aws-java-sdk-bundle:1.12.180") \
        # .config("spark.sql.warehouse.dir", "/work/delta") \
    return builder

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

def list_s3_files_using_resource(bucketName, folderName, today_only = False):
    """
    This functions list files from s3 bucket using s3 resource object.
    :return: list
    """

    from datetime import datetime

    today = datetime.now().replace(tzinfo = None)
    tdt = str(today.year) + '_' + str(today.month) + '_' + str(today.day)
    
    fileList= []
    s3_resource = boto3.resource("s3")
    s3_bucket = s3_resource.Bucket(bucketName)
    files = s3_bucket.objects.all()
    for file in files:
        if (file.key.startswith(folderName + '/') and file.key.endswith('.json')):
            flmd = str(file.last_modified.year) + '_' + str(file.last_modified.month) + '_' + str(file.last_modified.day)
            if (tdt == flmd) and today_only:
                fileList.append(file.key)
                # print(file.last_modified)
            else: 
                fileList.append(file.key)

    return fileList

def df2list(df, onePatientID, take='all'):
    
    """
    This function converts data frames to a list 

    inputs: 
            df - data frame containing attributes to be converted to a list
            onePatientID - the patient ID to be used for filtering 
            take - defaulted to take all records from the data frame but can be adjusted as needed
    output:
            lst - a list containing all the fields 

    """
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

    """
    Define schemas for use by spark data frames later
    """

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

# Plot utility  
def plot_graphs(datain, ylabel, saveto='plt', xlim=10):

    from matplotlib.ticker import AutoMinorLocator, FixedLocator, FixedFormatter

    x=range(0,len(datain[0]))
    plt.plot(x, datain[0], marker="x")

    # plt.gca().invert_xaxis()
    # plt.gca().invert_yaxis()
    # plt.yscale("log")
    plt.xlabel("Matches")
    plt.ylabel(ylabel)
    plt.legend(saveto)
    
    ax = plt.gca()
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.yaxis.set_minor_locator(AutoMinorLocator())

    ax.tick_params(axis="x", labelsize=7, labelrotation=-25, labelcolor="black")

    x_locator = FixedLocator(x)
    ax.xaxis.set_major_locator(x_locator)

    x_formatter = FixedFormatter(datain[1])
    ax.xaxis.set_major_formatter(x_formatter)

    ax.grid(axis="x", color="green", alpha=.3, linewidth=2, linestyle=":")
    ax.grid(axis="y", color="black", alpha=.5, linewidth=.5)

    ax.set_xlim(0, xlim)

    plt.savefig(saveto+'.png')

def random_strings(strlength=3):

    import string
    import random
    
    # initializing size of string
    N = strlength
    
    # using random.choices()
    # generating random strings
    res = ''.join(random.choices(string.ascii_uppercase +
                                string.digits, k=N))
    
    # print result
    return str(res)

def get_rand_mask():
    
    import random

    masks=['./patient_mask.png','./encounter_mask.png', './condition_mask.png']
    
    rand_idx = random.randint(0, len(masks)-1)

    return (masks[rand_idx])

def main():
    s3BucketName = 'synthea-output'
    s3FolderName =  'Bundles' # 'dupes'

    # get a list of all the data files for looping 
    fileList = list_s3_files_using_resource(s3BucketName, s3FolderName, use_today=True)

if __name__ == "__main__":
    main()