from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.store.models import NaverStore
from store.naver import get_products

app = create_app()
with app.app_context():
    store = NaverStore.query.filter_by(store_name="페브스토어").first()
    if store:
        data = get_products(page=1, size=100, client_id=store.client_id, client_secret=store.client_secret)
        # 페이지네이션 관련 키 전부 출력
        for k, v in data.items():
            if k != "contents":
                print(f"{k}: {v}")
        print(f"contents 개수: {len(data.get('contents', []))}")
