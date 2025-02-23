import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "bovespa_data")

BASE_URL = "https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/"

PAYLOAD_TEMPLATE = {
    "language": "pt-br",
    "pageNumber": 1,
    "pageSize": 20,
    "index": "IBOV",
    "segment": "1"
}
