import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "bovespa_data")
GLUE_SCRIPTS_DIR = "app/services/glue"

BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"

PAYLOAD_TEMPLATE = {
    "language": "pt-br",
    "pageNumber": 1,
    "pageSize": 20,
    "index": "IBOV",
    "segment": "1"
}

BUCKET_NAME = os.getenv("BUCKET_NAME")

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET")
AWS_SESSION = os.getenv("AWS_SESSION")
