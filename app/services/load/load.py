from app.services.load.helper import upload_directory_to_s3
from app.config import BUCKET_NAME, OUTPUT_DIR


if __name__ == "__main__":
    upload_directory_to_s3(OUTPUT_DIR, BUCKET_NAME)
