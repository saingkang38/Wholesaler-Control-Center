from pathlib import Path
from dotenv import load_dotenv

base = Path(__file__).resolve().parent
load_dotenv(base / ".env", override=True)
load_dotenv(base / ".env.local", override=True)

from app import create_app

app = create_app()

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)