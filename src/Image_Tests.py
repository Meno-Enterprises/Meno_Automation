from PIL import Image

save_path = '//192.168.0.178/meno/Hotfolders/PR--JOB-420_1-1_LTWBK-3060-MBA-somestuffblahblah_d9f94d7d03f5405da7c111ea82b6cf29__1.jpg'


im = Image.open(save_path)
print(im.info.get('adobe'))
#print(im.info.get('icc_profile'))
print(im.info.get('exif'))
print(im.mode)
print(im.palette)
print(im.has_transparency_data)
im.show()