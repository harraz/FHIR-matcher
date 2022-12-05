
import numpy as np
from PIL import Image, ImageDraw
from matcher import *

def to_image(stringlist, maskImage):

    imsize = (200, 200)

    s = "\n".join(map(str, stringlist))

    mask = Image.open('./' + maskImage).convert(mode='L')
    mask = mask.resize(size=imsize)

    img = Image.new(size=imsize, mode='L')
    img = Image.blend(mask, img,alpha=1)
    d = ImageDraw.Draw(img)
    d.multiline_text((0, 0), text=s.lower(), fill='white', align='left')

    return img

def pil_grid(images, max_horiz=np.iinfo(int).max):
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

    img= to_image(['hello','there','its','me'],'encounter_mask.png')
    img.save('./imgs/test.png')

    img2= to_image(['I','love','to see this','working'], 'condition_mask.png')
    img2.save('./imgs/test1.png')

    img3=pil_grid([img,img2],2)
    img3.save('./imgs/test2.png')

    # print(img_diff_ex('./imgs/test2.png'), 'rmse')

if __name__ == "__main__":
    main()