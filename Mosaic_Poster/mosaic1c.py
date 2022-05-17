#!/usr/bin/env python
# coding: utf-8

# In[1]:


import glob
from PIL import Image
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True
from scipy import spatial
import numpy as np
from PIL import Image, ImageEnhance
import random
from time import time

aspect_ratio_w = 800
aspect_ratio_h = 600
print('Tile aspect_ratio is: ',aspect_ratio_w/aspect_ratio_h)

#multiplication of logo
main_pic_mult = 120

#tile height
tile_height = 300
tile_width = int(np.round(tile_height*aspect_ratio_w/aspect_ratio_h))

# leng of color list 
random_choice_range = 0.15

# Sources and settings
main_photo_path = "image003.png"
tile_photos_path = "all_tiles\\*"


# new tile size
tile_size = (tile_width, tile_height)

# Get all tiles
print('Analysis of tile folder....')
start_time = time()
tile_paths = []
for file in glob.glob(tile_photos_path):
    tile_paths.append(file)
total = time()-start_time
print("Time spend on processing:", total)

# Import and resize all tiles
print('Reading images and calculation of average color for each tile....')
colors  = []
start_time = time()
for path in tile_paths:
    tile = Image.open(path)
    tile = tile.convert('RGBA')
    #tile = tile.resize(tile_size)
    mean_color = np.array(tile).mean(axis=0).mean(axis=0)
    colors.append(mean_color)
total = time()-start_time
print("Time spend on processing:", total)

# Pixelate (resize) main photo
print('Loading base image and resizing ....')
image = Image.open(main_photo_path)
image = image.convert('RGBA')
new_image = Image.new("RGBA", image.size, "WHITE") # Create a white rgba background
new_image.paste(image, (0, 0), image)              # Paste the image on the background. Go to the links given below for details.
main_photo = new_image

main_photo = main_photo.resize((main_photo.width*main_pic_mult,main_photo.height*main_pic_mult))

print('Main pic, old aspect ratio: ', main_photo.width/main_photo.height)  

width = int(np.round(main_photo.size[0] / tile_size[0]))
height = int(np.round(main_photo.size[1] / tile_size[1]))

print('Width of mosaic: ', width ,' tiles')
print('Width of mosaic: ', height ,' tiles')
resized_photo = main_photo.resize((width, height))

# Find closest tile photo for every pixel

# Create a KDTree
print('Construction of sparial color tree ....')
start_time = time()
tree = spatial.KDTree(colors)
total = time()-start_time
print("Time spend on processing:", total)


# Empty integer array to store indices of tiles
closest_tiles = np.zeros((width, height), dtype=np.uint32)
#closest Ntiles for each color
Nchoice = round(len(tile_paths)*random_choice_range)
print(Nchoice)

print('Tile selection ....')
start_time = time()
for i in range(width):
    for j in range(height):
        #select brightness facto
        #scale_factor = grades[m]
        pixel = resized_photo.getpixel((i, j))  # Get the pixel color at (i, j)
        distance, color_index_list = tree.query(pixel,k=Nchoice) # Returns (distance, index)
        color = np.random.choice(color_index_list, 1)[0] #random choice           
        closest_tiles[i, j] = color       # We only need the index
total = time()-start_time
print("Time spend on processing:", total)
        
# Create an output image
output = Image.new('RGB', main_photo.size)

print('Drawing mosaic ....')
start_time = time()
# Draw tiles
for i in range(width):
    for j in range(height):
        # Offset of tile
        x, y = i*tile_size[0], j*tile_size[1]
        # Index of tile
        index = closest_tiles[i, j]
        #open tile file and resize it
        #print(tile_paths[index])
        tile = Image.open(tile_paths[index])
        tile = tile.convert('RGBA')
        tile = tile.resize(tile_size)
        # Draw tile
        output.paste(tile, (x, y))
total = time()-start_time
print("Time spend on processing:", total)

# Save output
print('Saving png file ....')
start_time = time()
output.save("output_1.png")
total = time()-start_time
print("Time spend on processing:", total)


# In[ ]:




