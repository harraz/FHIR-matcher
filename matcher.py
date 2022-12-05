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
    p_diff_ratio= (p_diff_arr.sum()/im_size) ** 1

    enc_diff = ImageChops.difference(enc2, enc1)
    enc_diff_arr = np.asarray(np.array(enc_diff), dtype=np.float32)
    im_size = (enc_diff_arr.size) #(im1.size[0] * im1.size[1]) 
    enc_diff_ratio= (enc_diff_arr.sum()/im_size) * 0

    cond_diff = ImageChops.difference(cond2, cond1)
    econd_diff_arr = np.asarray(np.array(cond_diff), dtype=np.float32)
    im_size = (econd_diff_arr.size) #(im1.size[0] * im1.size[1]) 
    cond_diff_ratio= (econd_diff_arr.sum()/im_size)

    diff_ratio = (p_diff_ratio + enc_diff_ratio + cond_diff_ratio)/3

    return diff_ratio

def img_diff_ex(im1_path, im2_path, similarity_method='diffratio'):

    im1 = Image.open(im1_path)
    im2 = Image.open(im2_path)

    if (similarity_method != 'scc'):
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
    elif similarity_method == 'scc':
        img1_arr = np.asarray(np.array(im1), dtype=np.int8)
        img2_arr = np.asarray(np.array(im2), dtype=np.int8)
        similarity_measure = scc(img1_arr, img2_arr)
    else:
        similarity_measure = difference_ratio_by_parts(im1_path, im2_path)

    return similarity_measure

def sim_ratio_by_parts(im1_path, im2_path, similarity_method):

    p_similarity_measure=0
    enc_similarity_measure=0
    cond_similarity_measure=0

    p1, enc1, cond1= img_parts(im1_path, returnrray=True)
    p2, enc2, cond2= img_parts(im2_path, returnrray=True)

    if similarity_method == 'ergas':
        p_similarity_measure = ergas(p1, p2, ws=4)
        enc_similarity_measure = ergas(enc1, enc2, ws=4)
        cond_similarity_measure = ergas(cond1, cond2, ws=4)
    elif similarity_method == "sam": # not returnig good for some reason
        p_similarity_measure = sam(p1, p2)
        enc_similarity_measure = sam(enc1, enc2)
        cond_similarity_measure = sam(cond1, cond2)
    elif similarity_method == "uqi": # not sensitive to name variations
        p_similarity_measure = uqi(p1, p2)
        enc_similarity_measure = uqi(enc1, enc2)
        cond_similarity_measure = uqi(cond1, cond2)
    elif similarity_method == 'rmse': # not very accurate as the numberical value can generate noise
        p_similarity_measure = rmse(p1, p2)
        enc_similarity_measure = rmse(enc1, enc2)
        cond_similarity_measure = rmse(cond1, cond2)
    elif similarity_method == 'ssim': # not very sensitive and produce very close results and s;pw
        p_similarity_measure = np.average(ssim(p1, p2))
        enc_similarity_measure = np.average(ssim(enc1, enc2))
        cond_similarity_measure = np.average(ssim(cond1, cond2))
    elif similarity_method == 'psnr': # not sensitive to name variations
        p_similarity_measure = psnr(p1, p2)
        enc_similarity_measure = psnr(enc1, enc2)
        cond_similarity_measure = psnr(cond1, cond2)
    elif similarity_method == 'msssim':  # not sensitive to name variations and slow
        p_similarity_measure = msssim(p1, p2)
        enc_similarity_measure = msssim(enc1, enc2)
        cond_similarity_measure = msssim(cond1, cond2)
    elif similarity_method == 'scc':  # not sensitive to name variations
        p_similarity_measure = scc(p1, p2)
        enc_similarity_measure = scc(enc1, enc2)
        cond_similarity_measure = scc(cond1, cond2)
    elif similarity_method == 'vifp': # very slow but accurte and reversed
        p_similarity_measure = vifp(p1, p2)
        enc_similarity_measure = vifp(enc1, enc2)
        cond_similarity_measure = vifp(cond1, cond2)
    else:
        diff_ratio = difference_ratio_by_parts(im1_path, im2_path)

    diff_ratio = (p_similarity_measure ** 2 + enc_similarity_measure *0.5 + cond_similarity_measure *0.5)

    return diff_ratio

def img_parts(im1_path, returnrray=False):

    im1 = Image.open(im1_path)

    patient_crop = im1.crop((0,0,200,200))
    # patient_crop.save('./imgs/test3.png')

    encounter_crop = im1.crop((200,0,400,200))
    # encounter_crop.save('./imgs/test4.png')

    condition_crop = im1.crop((400,0,800,200))
    # condition_crop.save('./imgs/test5.png')

    if returnrray:    
        patient_crop =np.asarray(np.array(patient_crop), dtype=np.int8)
        encounter_crop =np.asarray(np.array(encounter_crop), dtype=np.int8)
        condition_crop =np.asarray(np.array(condition_crop), dtype=np.int8)

    return patient_crop, encounter_crop, condition_crop

def match_sorted(img_location, method, reverse_order=False):

    # similarity_methods= ['rmse', 'uqi', 'sam', 'ergas']

    # assign directory
    img_dir = './imgs/imgs_no_mask/'

    match_score={}
    # iterate over files image directory
    for filename in os.listdir(img_dir):
        if filename.endswith ('png') :
            # for method in similarity_methods:
            # diff=img_diff_ex(img_location, './imgs/' + filename, method)
            diff=sim_ratio_by_parts(img_location, img_dir + filename, method)
            # diff=img_diff(img_location, './imgs/' + filename)
            match_score[filename]=diff, method

    return sorted(match_score.items(), key=lambda x:x[1], reverse=reverse_order)

def main():

    simmethod = 'ergas' #'scc' #'psnr' #'vifp' #'ssim' #'dffratio'  #'sam' 'rmse' 'uqi' 'ergas'  

    # record_to_find= './imgs/imgs_no_mask/grid77_5fd4e698-b40f-19ea-2204-4e9013576661.png'
    record_to_find= './imgs/imgs_no_mask/dupe23_8d4596dc-614c-ed64-bc63-bfed74ea6e4d.png'
    # record_to_find = './imgs/imgs_no_mask/grid77_81a701f2-ce1b-1119-171c-d939509ba8e5.png'

    match_dict=match_sorted(record_to_find, simmethod, reverse_order=(simmethod=='vifp' or simmethod=='uqi' ))

    # print(match_dict, lambda x:x[:])

    [print(i, end='\n') for i in match_dict]
    # [print(match_dict[j][1][0] - match_dict[j+1][1][0], end='\n') for j in range(0, len(match_dict)-1)]
    diff_data=[]
    scores=[]
    lbls=[]
    # for j in range(0, len(match_dict)-1):
    for j in range(0, 15):
        diff_data.append(match_dict[j][1][0] - match_dict[j+1][1][0])
        scores.append(match_dict[j][1][0])
        lbls.append(match_dict[j][0][0:15]) # extraact first 16 charachters from the image file name
    
    # for ntry in match_dict:
    #     if ntry[0]==record_to_find:
    #         pass

    # diff_data_srt = sorted(diff_data)
    # print(np.abs(diff_data))

    # plot_graphs(np.abs(diff_data), 'match score diff')
    plot_graphs([scores, lbls], 'match distance', simmethod)

if __name__ == "__main__":
    main()