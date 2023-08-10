
from PIL import Image
from PIL.ExifTags import TAGS

class ImageProcessor:
    @staticmethod
    def process_exif(path):
        
        address= None
        try:
            # Open the image file
            image = Image.open(path)

            # Extract and display only GPSInfo metadata (if available)
            exif_data = image._getexif()
            if exif_data is not None:
                print(f"GPSInfo for {path}:")
                for tag, value in exif_data.items():
                    tag_name = TAGS.get(tag, tag)
                    if tag_name == "GPSInfo":
                    
                        latitude = value[2][0]
                        longitude = value[4][0]
                        # address = reverse_geocode(latitude, longitude)
                        # print(f"{address}")

            # Close the image file
            image.close()

            

            return address

        except IOError as e:
            print(f"Unable to open image file {path}: {e}")
        except AttributeError:
            print(f"No EXIF metadata found for {path}")

