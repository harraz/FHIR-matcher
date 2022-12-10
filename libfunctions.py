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
        .config("spark.jars.packages", 
                    "io.delta:delta-core_2.12:1.1.0,"
                    "org.apache.hadoop:hadoop-aws:3.2.2,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.180") \
        .config('spark.executor.instances', 8) \
        .config("spark.executor.memory", "8g")
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

# Plot utility  
def plot_graphs(datain, ylabel, saveto='plt'):
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

    ax.set_xlim(-1, 10)

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