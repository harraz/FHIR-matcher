import boto3

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