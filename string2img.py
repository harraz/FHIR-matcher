
import numpy as np
from PIL import Image, ImageDraw

def to_image(stringlist):

    s = "\n".join(map(str, stringlist))

    img = Image.new(size=(1700, 1700), mode='L')
    d = ImageDraw.Draw(img)
    d.multiline_text((0, 0), text=s.lower(), fill='white', align='left')

    img.show()

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
    im_grid = Image.new('RGB', (h_sizes[-1], v_sizes[-1]), color='white')
    for i, im in enumerate(images):
        im_grid.paste(im, (h_sizes[i % n_horiz], v_sizes[i // n_horiz]))
    return im_grid

def main():

    img= to_image(['hello','there','its','me'])
    img.save('./imgs/test.png')

    img2= to_image(['I','love','to see this','working'])
    img2.save('./imgs/test1.png')

    pil_grid([img,img2],2).save('./imgs/test2.png')

if __name__ == "__main__":
    main()