import os
import sys
import subprocess
import time
from pathlib import Path
from dotenv import load_dotenv

base = Path(__file__).resolve().parent
load_dotenv(base / ".env", override=True)
load_dotenv(base / ".env.local", override=True)


# ─────────────────────────────────────────────────────────────────────────────
# 서버 식별 배너 — 다른 프로젝트 콘솔과 헷갈리지 않게 명확히 표시
# ─────────────────────────────────────────────────────────────────────────────

# ANSI 색 (Windows 10+ 콘솔에서 동작)
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _enable_ansi_colors_windows():
    """Windows 콘솔에 ANSI escape 처리 활성화 — 색이 안 들어오면 무시."""
    if os.name != "nt":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
        h = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = ctypes.c_ulong()
        if kernel32.GetConsoleMode(h, ctypes.byref(mode)):
            kernel32.SetConsoleMode(h, mode.value | ENABLE_VIRTUAL_TERMINAL_PROCESSING)
    except Exception:
        pass


def _set_console_title_windows(title: str):
    """Windows 콘솔 창 제목줄 설정 — 작업표시줄에서 어떤 서버인지 식별."""
    if os.name != "nt":
        return
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleTitleW(title)
    except Exception:
        pass


def _print_server_banner(port: int):
    """서버 식별용 큰 배너 + 정보 블록 출력."""
    _enable_ansi_colors_windows()
    _set_console_title_windows(f"[Wholesaler] 도매처 통합 관리 서버 -- port {port}")

    R = _RED
    B = _BOLD
    N = _RESET
    line = "#" * 78

    banner = f"""
{R}{line}{N}
{R}#{N}                                                                            {R}#{N}
{R}#{N}     {B}W H O L E S A L E R   C O N T R O L   C E N T E R{N}                  {R}#{N}
{R}#{N}                                                                            {R}#{N}
{R}#{N}              {B}도매처 통합 관리 시스템{N}  --  port {B}{port}{N}                       {R}#{N}
{R}#{N}                                                                            {R}#{N}
{R}{line}{N}

  디렉토리  : {base}
  파이썬    : py -3.12
  접속 URL  : http://localhost:{port}
  PID       : {os.getpid()}

  {R}* 이 창을 닫으면 서버가 종료됩니다.{N}
  {R}* 빨간색 배너 = 도매처 통합 관리 서버 창 (다른 프로젝트와 혼동 주의){N}

{'-' * 78}
"""
    print(banner, flush=True)


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

    # 식별 배너 — 콘솔 창 제목 + 큰 배너 (다른 프로젝트 서버와 헷갈림 방지)
    _print_server_banner(PORT)

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
