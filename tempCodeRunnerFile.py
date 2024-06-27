from PIL import Image

# Open the four images
img1 = Image.open("/mnt/data/file-ISV5aHLmiV2aGwSDWWFOmfGe")
img2 = Image.open("/mnt/data/file-TYttqRpTAEZaprxbEoF5GpQx")
img3 = Image.open("/mnt/data/file-WwhjaWKs9DlRjsoi7fNuF2Jg")
img4 = Image.open("/mnt/data/file-tKShEFAAlUvDygiewIPp3lZR")

# Define size for the output image
width, height = img1.size

# Create a new image with double width and height of the input images
new_img = Image.new('RGB', (2 * width, 2 * height))

# Paste the images into the new image
new_img.paste(img1, (0, 0))
new_img.paste(img2, (width, 0))
new_img.paste(img3, (0, height))
new_img.paste(img4, (width, height))

# Save the combined image
output_path = "D:\\Github anyud\\final\\.csvcombined_image_final.png"
new_img.save(output_path)

import ace_tools as tools; tools.display_image(new_img, "Combined Image 2x2")
