import boto3
from pyspark.sql.types import StructType, StructField, StringType, DateType
import matplotlib.pyplot as plt

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