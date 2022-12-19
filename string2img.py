
import numpy as np
from PIL import Image, ImageDraw
from matcher import *

def to_image(stringlist, maskImage):

    """
    This function takes in a list of strings and returns b/w image of size 200x200 pixles
    The function can blend a second image 

    stringList: List of strings to be converted to image
    maskImage: PIL Image to be blended with final image
    """

    # fix image size
    imsize = (200, 200)

    # use str function to map string list into a string with new lines
    s = "\n".join(map(str, stringlist))

    # open the mask image in b/w mode and resize it
    mask = Image.open('./' + maskImage).convert(mode='L')
    mask = mask.resize(size=imsize)

    # create a new b/w image of fixed size and blend with mask 
    img = Image.new(size=imsize, mode='L')
    img = Image.blend(mask, img,alpha=1) # note that alpha is hardcoded to 1 here since it's no longer needed but can be parameterized if needed
    d = ImageDraw.Draw(img)

    # paset the text from string onto the final image
    d.multiline_text((0, 0), text=s.lower(), fill='white', align='left')

    # clean up
    mask.close()

    return img

def pil_grid(images, max_horiz=np.iinfo(int).max):

    """
    This function combines images into a grid of rows and columns based on the max_horiz argument
    It was borrowed from this stackoverflow: https://stackoverflow.com/questions/30227466/combine-several-images-horizontally-with-python
    It's magical!
    """
    n_images = len(images)
    n_horiz = min(n_images, max_horiz)
    h_sizes, v_sizes = [0] * n_horiz, [0] * (n_images // n_horiz)
    for i, im in enumerate(images):
        h, v = i % n_horiz, i // n_horiz
        h_sizes[h] = max(h_sizes[h], im.size[0])
        v_sizes[v] = max(v_sizes[v], im.size[1])
    h_sizes, v_sizes = np.cumsum([0] + h_sizes), np.cumsum([0] + v_sizes)
    im_grid = Image.new('L', (h_sizes[-1], v_sizes[-1]), color='white')
    for i, im in enumerate(images):
        im_grid.paste(im, (h_sizes[i % n_horiz], v_sizes[i // n_horiz]))
    return im_grid

def main():

    # pass
    # diff=difference_ratio_by_parts('./imgs/dupe_5fd4e698-b40f-19ea-2204-4e9013576661.png', './imgs/grid3_5fd4e698-b40f-19ea-2204-4e9013576661.png')
    # print(diff)

    img= to_image(['hello','there','its','me'], './encounter_mask.png')
    img.save('./imgs/test.png')
    print(img.getbbox())

    img2= to_image(['I','love','to see this','working'], './condition_mask.png')
    img2.save('./imgs/test1.png')
    print(img2.getbbox())

    img3=pil_grid([img,img2],2)
    img3.save('./imgs/test2.png')

    # print(img_diff_ex('./imgs/test2.png'), 'rmse')

if __name__ == "__main__":
    main()