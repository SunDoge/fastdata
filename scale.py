import pyvips

# img = pyvips.Image.new_from_file("/mnt/cephfs/mixed/dataset/imagenet/val/n01440764/ILSVRC2012_val_00002138.JPEG")
img = pyvips.Image.new_from_file("beforescale.jpg")

# img = img.colourspace(pyvips.enums.Interpretation.GREY16)
img = img.colourspace(pyvips.enums.Interpretation.SRGB)
print(img.width, img.height)
scale = 256 / min(img.width, img.height)
print(scale)
img = img.resize(scale, kernel=pyvips.enums.Kernel.LINEAR)

img.write_to_file('out.jpg')
