from string2img import *
from libfunctions import *

# s3BucketName1='synthea-output'
# s3FolderName1= 'dupes' #'Bundles'
# fileList1 = list_s3_files_using_resource(s3BucketName1, s3FolderName1)

# s3BucketName2='synthea-output'
# s3FolderName2= 'Bundles'
# fileList2 = list_s3_files_using_resource(s3BucketName2, s3FolderName2)

# s3 = boto3.resource('s3')
# i=0
# for fileName in fileList1:

#     content_object = s3.Object(s3BucketName1, fileName)
#     print(fileName)

import os

def match_sorted():
    # assign directory
    img_dir = './imgs'

    match_score={}
    # iterate over files image directory
    for filename in os.listdir(img_dir):
        if filename.startswith ('grid3_'):
            diff=img_diff('./imgs/dupe_4b39cfef-642f-ecd8-6742-5991b37aaa93.png', './imgs/' + filename)
            match_score[filename]=diff

    return sorted(match_score.items(), key=lambda x:x[1],reverse=False)

def main():

    match_dict=match_sorted()

    # print(match_dict, lambda x:x[:])

    [print(i, end='\n') for i in match_dict]


if __name__ == "__main__":
    main()