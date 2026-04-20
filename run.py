import os
import sys
import subprocess
import time
from pathlib import Path
from dotenv import load_dotenv

base = Path(__file__).resolve().parent
load_dotenv(base / ".env", override=True)
load_dotenv(base / ".env.local", override=True)


def _cleanup_stuck_runs():
    """이전 서버 크래시로 running 상태에 걸린 collection_runs를 failed로 정리."""
    import sqlite3
    db_path = base / "instance" / "wholesaler.db"
    if not db_path.exists():
        return
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        result = conn.execute(
            "UPDATE collection_runs SET status='failed', finished_at=datetime('now'), "
            "error_summary='서버 재시작으로 인한 강제 종료' WHERE status='running'"
        )
        conn.commit()
        if result.rowcount > 0:
            print(f"[run] stuck 수집 레코드 {result.rowcount}건 정리")
        conn.close()
    except Exception as e:
        print(f"[run] DB 정리 실패 (무시): {e}")


def _cleanup_orphan_browsers():
    """이전 OOM 크래시로 생존한 고아 Playwright Chromium 프로세스 종료."""
    try:
        result = subprocess.run(
            'wmic process where "ExecutablePath like \'%ms-playwright%\'" call terminate',
            shell=True, capture_output=True, text=True, timeout=10
        )
        if result.stdout and "No Instance(s) Available" not in result.stdout and "ReturnValue = 0" in result.stdout:
            print("[run] 고아 Playwright Chromium 프로세스 정리 완료")
    except Exception as e:
        print(f"[run] Chromium 정리 실패 (무시): {e}")


from app import create_app

app = create_app()

if __name__ == "__main__":
    PORT = 5000
    PID_FILE = base / "server.pid"

    # 이전 서버 PID 종료 (graceful → force 순서)
    if PID_FILE.exists():
        old_pid = PID_FILE.read_text().strip()
        if old_pid.isdigit():
            # 1) 일반 종료 시도 (트랜잭션 완료 대기)
            subprocess.run(f'taskkill /PID {old_pid}', shell=True, capture_output=True)
            time.sleep(2)
            # 2) 아직 살아있으면 강제 종료
            still = subprocess.run(
                f'tasklist /FI "PID eq {old_pid}"',
                shell=True, capture_output=True, text=True
            )
            if old_pid in still.stdout:
                subprocess.run(f'taskkill /F /PID {old_pid}', shell=True, capture_output=True)
                print(f"[run] 이전 서버 (PID {old_pid}) 강제 종료")
                time.sleep(1)
            else:
                print(f"[run] 이전 서버 (PID {old_pid}) 정상 종료")
        PID_FILE.unlink()

    # DB stuck 레코드 정리 + 고아 Chromium 프로세스 정리
    _cleanup_stuck_runs()
    _cleanup_orphan_browsers()

    # 현재 PID 기록
    PID_FILE.write_text(str(os.getpid()))
    print(f"[run] 서버 PID: {os.getpid()} → server.pid 저장")

    from waitress import serve
    print(f"[run] Waitress 서버 시작 (0.0.0.0:{PORT}, threads=8)")
    try:
        serve(app, host="0.0.0.0", port=PORT, threads=8)
    finally:
        if PID_FILE.exists() and PID_FILE.read_text().strip() == str(os.getpid()):
            PID_FILE.unlink()
