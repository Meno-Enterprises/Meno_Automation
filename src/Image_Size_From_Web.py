import requests
from PIL import Image

def download_image(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as file:
        file.write(response.content)

def get_image_info(image_path):
    image = Image.open(image_path)
    size = image.size
    width = image.width
    dpi = image.info.get('dpi')
    return size, width, dpi

# Example usage
url = 'https://northwestimprints.com/orders-completed/IF0017901-3-1NHL695010029RET.jpg'
save_path = './image.jpg'

download_image(url, save_path)
size, width, dpi = get_image_info(save_path)

print(f"Image size: {size}")
print(f"Image width: {width}")
print(f"Image DPI: {dpi}")
inch_size = (size[0] / dpi[0], size[1] / dpi[1])
inch_width = width / dpi[0]

print(f"Image size (in inches): {inch_size}")
print(f"Image width (in inches): {inch_width}")