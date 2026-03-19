from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.store.models import StoreProduct
from app.master.models import MasterProduct

app = create_app()
with app.app_context():
    stores = StoreProduct.query.all()
    print("=== 스토어 상품 판매자관리코드 (상위 10개) ===")
    for s in stores[:10]:
        print(f"  origin_no={s.origin_product_no}, code='{s.seller_management_code}', matched={s.master_product_id}")

    masters = MasterProduct.query.limit(10).all()
    print("\n=== 마스터 상품 supplier_product_code (상위 10개) ===")
    for m in masters[:10]:
        print(f"  id={m.id}, code='{m.supplier_product_code}'")
