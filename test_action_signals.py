from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.actions import detect_action_signals

app = create_app()
with app.app_context():
    stats = detect_action_signals(wholesaler_id=1)
    print(stats)
