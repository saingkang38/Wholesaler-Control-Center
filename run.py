import sys
import socket
from pathlib import Path
from dotenv import load_dotenv

base = Path(__file__).resolve().parent
load_dotenv(base / ".env", override=True)
load_dotenv(base / ".env.local", override=True)

from app import create_app

app = create_app()

if __name__ == "__main__":
    PORT = 5000

    # 중복 실행 방지: 포트가 이미 사용 중이면 종료
    _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        _sock.bind(("0.0.0.0", PORT))
        _sock.close()
    except OSError:
        print(f"[run] 포트 {PORT} 이미 사용 중 — Flask가 이미 실행 중입니다. 종료합니다.")
        sys.exit(1)

    from waitress import serve
    print(f"[run] Waitress 서버 시작 (0.0.0.0:{PORT}, threads=8)")
    serve(app, host="0.0.0.0", port=PORT, threads=8)