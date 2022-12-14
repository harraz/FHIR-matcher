from string2img import *
from libfunctions import *
from sewar.full_ref import mse, rmse, psnr, uqi, ssim, ergas, scc, rase, sam, msssim, vifp
from PIL import Image, ImageChops
import os
import numpy as np

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

def difference_ratio_by_parts_rowsum(test_imag, im2_path):

    def calc_rowsumratio(test_img, ref_img):

        """ Comparaes two images and returns a difference ratio
        The function trys to find the difference between the test and reference image using PIL difference method
        It uses the test image size to pick the same area and location in the reference image for difference calculation
        """

        crop_areea, ref_img_crop_area, test_img_crop_area = None, 0, 0

        # calculate corp size
        if (ref_img.getbbox() != None):
            ref_img_crop_area = (ref_img.getbbox()[3] - ref_img.getbbox()[1]) * (ref_img.getbbox()[2] - ref_img.getbbox()[0])
        if (test_img.getbbox() != None):
            test_img_crop_area = (test_img.getbbox()[3] - test_img.getbbox()[1]) * (test_img.getbbox()[2] - test_img.getbbox()[0])


        if (ref_img_crop_area >= test_img_crop_area):
            crop_areea=test_img.getbbox()
        else:
            crop_areea=ref_img.getbbox()

        # calculate the difference between the test image and the same area in the reference image
        # img_diff = ImageChops.difference(test_img.crop(test_img.getbbox()), ref_img.crop(test_img.getbbox()))
        img_diff = ImageChops.difference(test_img.crop(crop_areea), ref_img.crop(crop_areea))

        # convert the difference to an np array
        img_diff_arr = np.asarray(np.array(img_diff), dtype=np.float32)
        # img_diff.save('img_diff.png')

        # crop an area of the image equal to the test image
        # convert to np array
        ref_img_crop = ref_img.crop(test_img.getbbox())
        ref_img_arr = np.asarray(np.array(ref_img_crop), dtype=np.float32)
        # ref_img_crop.save('ref_img_crop.png')

        # convert all values 1 for simpler calculations
        img_diff_arr[img_diff_arr[:][:]==255]=1.0
        ref_img_arr[ref_img_arr[:][:]==255]=1.0

        # sum all the values in the difference image
        a=np.sum(img_diff_arr) 
        
        # summ all the values in the test reference image
        b=np.sum(ref_img_arr)

        if (b==np.inf or b==0):
            b=0.1

        # calculate the diffence ratio in percent between the difference and the reference image
        img_diff_ratio = np.divide(a, b) * 100

        return img_diff_ratio

    p1, enc1, cond1= img_parts(test_imag)
    p2, enc2, cond2= img_parts(im2_path)

    p_diff_ratio = calc_rowsumratio(p1, p2)

    enc_diff_ratio = calc_rowsumratio(enc1, enc2)

    cond_diff_ratio = calc_rowsumratio(cond1, cond2)

    return p_diff_ratio , enc_diff_ratio, cond_diff_ratio

def difference_ratio_rowsum(test_imag, im2_path):

    im1 = Image.open(test_imag)
    im2 = Image.open(im2_path)

    diff = ImageChops.difference(im2, im1)
    # diff.show()
    diff_arr = np.asarray(np.array(diff), dtype=np.float32)

    im_size = (diff_arr.size) #(im1.size[0] * im1.size[1]) 

    diff_ratio= (diff_arr.sum()/im_size)
    # diff_ratio =np.log2(diff_arr.sum())

    diff_arr[diff_arr[:][:]==255]=1
    rowsum=np.sum(diff_arr[:][:], axis=1) / diff_arr.shape[1]

    diff_ratio1 = np.sum(rowsum)

    return diff_ratio1

def difference_ratio_box(im1_path, im2_path):

    im1 = Image.open(im1_path)
    im2 = Image.open(im2_path)

    diff = ImageChops.difference(im2, im1)
    diff_arr = np.asarray(np.array(diff), dtype=np.float32)

    im_size = (diff_arr.size) #(im1.size[0] * im1.size[1]) 

    diff_ratio= (diff_arr.sum()/im_size)

    diff.save('diff.png')

    # print(im2_path, im1.getbbox(), im2.getbbox(), diff.getbbox())

    return diff_ratio

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

    # diff_ratio = (p_diff_ratio + enc_diff_ratio + cond_diff_ratio)/3

    return p_diff_ratio , enc_diff_ratio, cond_diff_ratio

def img_diff_ex(test_imag, im2_path, similarity_method='diffratio'):

    im1 = Image.open(test_imag)
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
        # similarity_measure = difference_ratio(im1_path, im2_path)
        # similarity_measure = difference_ratio_box(im1_path, im2_path)
        # similarity_measure= difference_ratio_rowsum(im1_path, im2_path)
        similarity_measures = difference_ratio_by_parts_rowsum(test_imag, im2_path)
        similarity_measure = np.sum(similarity_measures) /3.0

        # print(im2_path, [similarity_measures], similarity_measure)

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
        p_similarity_measure, enc_similarity_measure, cond_similarity_measure = difference_ratio_by_parts(im1_path, im2_path)

    diff_ratio = (p_similarity_measure + enc_similarity_measure  + cond_similarity_measure ) / 3.0
    print(im1_path, im2_path,p_similarity_measure, enc_similarity_measure, cond_similarity_measure)
    return diff_ratio

def img_parts(im1_path, returnrray=False):

    """ Given a path to an image, return even 200x200 parts of the image as individual images or np arrays
    the function currently returns 3 parts and assumes the given image in 
    """

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

def match_sorted(test_img_path, ref_img_path, method, reverse_order=False):

    """Given a test image and a reference image, this function calls img_diff_ex() 
    to calculate the similarity score between the test image and all the images in the refernece image location provided
    
        inputs:
        test_img_path - path to a PNG image to test similarity
        ref_img_path - path to reference PNG images
        method - similariy method to be used
        reverse_order - order of the match score, not reversed by default

        output:
        sorted dictionary of the match scores in prder defined by the reverse_order argument
    """

    match_score={}
    # iterate over files in directory
    for filename in os.listdir(ref_img_path):
        if filename.endswith ('png') :
            # for method in similarity_methods:
            diff=img_diff_ex(test_img_path, ref_img_path + filename, method)
            # diff=sim_ratio_by_parts(img_location, img_dir + filename, method)
            # diff=img_diff(img_location, './imgs/' + filename)
            match_score[filename]=diff, method

    return sorted(match_score.items(), key=lambda x:x[1], reverse=reverse_order)

def main():

    simmethod = 'diffratio' #'scc' #'psnr' #'vifp' #'ssim' #'diffratio'  #'sam' 'rmse' 'uqi' 'ergas'  

    # record_to_find= './imgs/imgs_no_mask/dupe23_0b26b53a-d6b0-cf7b-5107-a6367c0b5d61.png' # 3 images found
    # record_to_find= './imgs/imgs_no_mask/dupe23_8d4clear596dc-614c-ed64-bc63-bfed74ea6e4d.png' # file does not exit is img location
    # record_to_find = './imgs/imgs_no_mask/grid77_81a701f2-ce1b-1119-171c-d939509ba8e5.png' # one dupe 
    record_to_find = './imgs/dupe23_38970255-c586-8e0e-f328-cbacb314780a.png' # diffration found one more than ergas
    # record_to_find= './imgs/imgs_no_mask/duper23_73119f15-a35c-d7fc-9fd2-e54d8d049226.png' # 3 images found
    # record_to_find= './imgs/imgs_no_mask/dupe23_4b39cfef-642f-ecd8-6742-5991b37aaa93.png' # 3 images found
    # record_to_find= './imgs/imgs_no_mask/duper23_a9855237-bdde-707e-cf2e-6de590b79d1d.png' # 3 
    # record_to_find ='./imgs/imgs_no_mask/duper23_8206939b-7317-e2f9-bfc8-34bf910f4eba.png' # 3 imags found
    # record_to_find= './imgs/tst153_b13cfbdc-00ab-bc8d-e493-9ae4a27bd512.png' # 3 found
    # record_to_find= './imgs/tst235_a9959419-fd3e-1ed5-cf4c-2887b6db1f39.png' # no duplicates
    record_to_find ='./imgs/tst375_a9959419-fd3e-1ed5-cf4c-2887b6db1f39.png' # no duplicates
    record_to_find = './imgs/tst375_f7039ec1-3281-7a16-7b71-ca667842669c.png'
    
    # ref_img_location = './imgs/imgs_no_mask/'
    ref_img_location = './imgs/'

    match_dict=match_sorted(record_to_find, ref_img_location, simmethod, reverse_order=(simmethod=='vifp' or simmethod=='uqi'))
    # match_dict=match_sorted(record_to_find, ref_img_location, 'ergas', reverse_order=(simmethod=='vifp' or simmethod=='uqi'))
    

    # print(match_dict, lambda x:x[:])

    [print(i, end='\n') for i in match_dict]
    # [print(match_dict[j][1][0] - match_dict[j+1][1][0], end='\n') for j in range(0, len(match_dict)-1)]
    diff_data=[]
    scores=[]
    lbls=[]
    # for j in range(0, len(match_dict)-1):
    for j in range(0, 20):
        diff_data.append(match_dict[j][1][0] - match_dict[j+1][1][0])
        scores.append(match_dict[j][1][0])
        lbls.append(match_dict[j][0][0:15]) # extraact first 16 charachters from the image file name

    frst_drv = np.diff(scores,1)
    scnd_drv = np.diff(scores,2)
    
    top_matches = match_dict[:np.argmax(frst_drv)+1]

    print(np.argmax(frst_drv), np.argmin(scnd_drv), list(zip(*top_matches))[0])

    display_name= os.path.basename(record_to_find)

    plot_graphs([scores, lbls], 'match distance - ' + (display_name[:16]), simmethod, xlim=10)
    plot_graphs([frst_drv, lbls], 'match distance - ' + (display_name[:16]), simmethod + '_drv', xlim=10)
    plot_graphs([scnd_drv, lbls], 'match distance - ' + (display_name[:16]), simmethod + '_2drv', xlim=10)

if __name__ == "__main__":
    main()