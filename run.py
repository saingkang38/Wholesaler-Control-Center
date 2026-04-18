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

    # 포트 사용 중이면 기존 프로세스 종료 후 재시작
    _sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        _sock.bind(("0.0.0.0", PORT))
        _sock.close()
    except OSError:
        print(f"[run] 포트 {PORT} 사용 중 → 기존 프로세스 종료 후 재시작")
        import subprocess, time
        # 포트 점유 PID 찾아서 종료
        try:
            result = subprocess.run(
                f'netstat -ano | findstr :{PORT}',
                shell=True, capture_output=True, text=True
            )
            pids = set()
            for line in result.stdout.splitlines():
                parts = line.split()
                if parts and parts[-1].isdigit():
                    pids.add(parts[-1])
            for pid in pids:
                subprocess.run(f'taskkill /F /PID {pid}', shell=True,
                               capture_output=True)
                print(f"[run] PID {pid} 종료")
            time.sleep(1)
        except Exception as e:
            print(f"[run] 기존 프로세스 종료 실패: {e}")
            sys.exit(1)

    from waitress import serve
    print(f"[run] Waitress 서버 시작 (0.0.0.0:{PORT}, threads=8)")
    serve(app, host="0.0.0.0", port=PORT, threads=8)