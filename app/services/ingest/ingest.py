from app.services.ingest.helper import upload_directory_to_s3, upload_glue_scripts_to_s3
from app.config import BUCKET_NAME, OUTPUT_DIR, GLUE_SCRIPTS_DIR


if __name__ == "__main__":
    upload_directory_to_s3(OUTPUT_DIR, BUCKET_NAME)
    upload_glue_scripts_to_s3(GLUE_SCRIPTS_DIR, BUCKET_NAME)
