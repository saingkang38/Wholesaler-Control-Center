from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.store import sync_store_products

app = create_app()
with app.app_context():
    stats = sync_store_products(wholesaler_id=1)
    print(stats)
