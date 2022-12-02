from string2img import *
from libfunctions import *
from sewar.full_ref import mse, rmse, psnr, uqi, ssim, ergas, scc, rase, sam, msssim, vifp
from PIL import Image, ImageChops
import os

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

def difference_ratio(im1_path, im2_path):

    im1 = Image.open(im1_path)
    im2 = Image.open(im2_path)

    diff = ImageChops.difference(im2, im1)
    # diff.show()
    diff_arr = np.asarray(np.array(diff), dtype=np.float32)

    im_size = (diff_arr.size) #(im1.size[0] * im1.size[1]) 

    diff_ratio= (diff_arr.sum()/im_size)
    # diff_ratio =np.log2(diff_arr.sum())

    return diff_ratio

def difference_ratio_by_parts(im1_path, im2_path):

    # im1 = Image.open(im1_path)
    # im2 = Image.open(im2_path)

    p1, enc1, cond1= img_parts(im1_path)
    p2, enc2, cond2= img_parts(im2_path)

    p_diff = ImageChops.difference(p2, p1)
    p_diff_arr = np.asarray(np.array(p_diff), dtype=np.float32)
    im_size = (p_diff_arr.size) #(im1.size[0] * im1.size[1]) 
    p_diff_ratio= (p_diff_arr.sum()/im_size) * 2

    enc_diff = ImageChops.difference(enc2, enc1)
    enc_diff_arr = np.asarray(np.array(enc_diff), dtype=np.float32)
    im_size = (enc_diff_arr.size) #(im1.size[0] * im1.size[1]) 
    enc_diff_ratio= (enc_diff_arr.sum()/im_size)

    cond_diff = ImageChops.difference(cond2, cond1)
    econd_diff_arr = np.asarray(np.array(cond_diff), dtype=np.float32)
    im_size = (econd_diff_arr.size) #(im1.size[0] * im1.size[1]) 
    cond_diff_ratio= (econd_diff_arr.sum()/im_size)

    diff_ratio = (p_diff_ratio + enc_diff_ratio + cond_diff_ratio)/3

    return diff_ratio

def img_diff_ex(im1_path, im2_path, similarity_method='diffratio'):

    im1 = Image.open(im1_path)
    im2 = Image.open(im2_path)

    img1_arr = np.asarray(np.array(im1), dtype=np.float32)
    img2_arr = np.asarray(np.array(im2), dtype=np.float32)
    
    if similarity_method == 'uqi':
        similarity_measure = uqi(img1_arr, img2_arr)
    elif similarity_method == "ergas":
        similarity_measure = ergas(img1_arr, img2_arr)
    elif similarity_method == "sam":
        similarity_measure = sam(img1_arr, img2_arr)
    elif similarity_method == 'rmse':
        similarity_measure = rmse(img1_arr, img2_arr)
    else:
        similarity_measure = difference_ratio_by_parts(im1_path, im2_path)

    return similarity_measure

def img_parts(im1_path):

    im1 = Image.open(im1_path)

    patient_crop = im1.crop((0,0,200,200))
    # patient_crop.save('./imgs/test3.png')

    encounter_crop = im1.crop((200,0,400,200))
    # encounter_crop.save('./imgs/test4.png')

    condition_crop = im1.crop((400,0,800,200))
    # condition_crop.save('./imgs/test5.png')
    
    # patient_crop_arr =np.asarray(np.array(patient_crop), dtype=np.float32)
    # encouter_crop_arr =np.asarray(np.array(encounter_crop), dtype=np.float32)
    # condition_crop_arr =np.asarray(np.array(condition_crop), dtype=np.float32)

    return patient_crop, encounter_crop, condition_crop
def match_sorted(img_location, method):

    # similarity_methods= ['rmse', 'uqi', 'sam', 'ergas']

    # assign directory
    img_dir = './imgs'

    match_score={}
    # iterate over files image directory
    for filename in os.listdir(img_dir):
        if filename.startswith ('grid3_'):
            # for method in similarity_methods:
            diff=img_diff_ex(img_location, './imgs/' + filename, method)
            # diff=img_diff(img_location, './imgs/' + filename)
            match_score[filename]=diff, method

    return sorted(match_score.items(), key=lambda x:x[1],reverse=False)

def main():

    simmethod = 'dffratio'  #'sam' 'rmse' # 'uqi' 'ergas'  

    match_dict=match_sorted('./imgs/dupe_44be8c5b-b55c-681f-6e0a-64f3af5c113b.png', simmethod)

    # print(match_dict, lambda x:x[:])

    [print(i, end='\n') for i in match_dict]
    # [print(match_dict[j][1][0] - match_dict[j+1][1][0], end='\n') for j in range(0, len(match_dict)-1)]
    diff_data=[]
    scores=[]
    for j in range(0, len(match_dict)-1):
        diff_data.append(match_dict[j][1][0] - match_dict[j+1][1][0])
        scores.append(match_dict[j][1][0])
    
    # diff_data_srt = sorted(diff_data)
    # print(np.abs(diff_data))

    # plot_graphs(np.abs(diff_data), 'match score diff')
    plot_graphs(scores, 'match distance', simmethod)

if __name__ == "__main__":
    main()