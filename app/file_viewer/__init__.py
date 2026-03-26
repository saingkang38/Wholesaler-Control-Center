import io
import zipfile
import datetime
from pathlib import Path
from flask import Blueprint, render_template, abort
from flask_login import login_required

file_viewer_bp = Blueprint("file_viewer", __name__)

DOWNLOADS_ROOT = Path(__file__).resolve().parents[2] / "downloads"

WHOLESALER_NAMES = {
    "ownerclan": "오너클랜",
    "feelwoo": "필우",
    "jtckorea": "JTC코리아",
    "metaldiy": "메탈DIY",
    "ds1008": "DS1008",
    "hitdesign": "힛디자인",
    "mro3": "3MRO",
    "sikjaje": "식자재",
    "onch3": "온채",
}


@file_viewer_bp.route("/downloads")
@login_required
def index():
    from app.wholesalers.models import Wholesaler

    wholesalers = Wholesaler.query.order_by(Wholesaler.name).all()
    folders = []
    for w in wholesalers:
        folder = DOWNLOADS_ROOT / w.code
        files = [f for f in folder.iterdir() if f.is_file()] if folder.exists() else []
        latest = max((f.stat().st_mtime for f in files), default=None)
        latest_str = datetime.datetime.fromtimestamp(latest).strftime("%Y-%m-%d %H:%M") if latest else "-"
        folders.append({
            "code": w.code,
            "name": w.name,
            "file_count": len(files),
            "latest": latest_str,
        })
    return render_template("file_viewer_index.html", folders=folders)


@file_viewer_bp.route("/downloads/<code>")
@login_required
def file_list(code):
    folder = (DOWNLOADS_ROOT / code).resolve()
    if not folder.is_relative_to(DOWNLOADS_ROOT) or not folder.exists() or not folder.is_dir():
        abort(404)
    files = []
    for f in sorted(folder.iterdir(), reverse=True):
        if f.is_file():
            size_kb = f.stat().st_size // 1024
            mtime = datetime.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            files.append({"name": f.name, "size_kb": size_kb, "mtime": mtime})
    return render_template(
        "file_viewer_list.html",
        code=code,
        wholesaler_name=WHOLESALER_NAMES.get(code, code),
        files=files,
    )


@file_viewer_bp.route("/downloads/<code>/<filename>/preview")
@login_required
def preview(code, filename):
    filepath = (DOWNLOADS_ROOT / code / filename).resolve()
    if not filepath.is_relative_to(DOWNLOADS_ROOT) or not filepath.exists():
        abort(404)

    ext = filepath.suffix.lower()
    tables = []

    try:
        if ext == ".zip":
            with zipfile.ZipFile(filepath, "r") as zf:
                xlsx_names = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
                for xname in xlsx_names:
                    with zf.open(xname) as xf:
                        data = xf.read()
                    rows = _read_xlsx_bytes(data)
                    tables.append({"title": xname, "rows": rows})
        elif ext == ".xlsx":
            rows = _read_xlsx_bytes(filepath.read_bytes())
            tables.append({"title": filename, "rows": rows})
        else:
            abort(400)
    except Exception as e:
        return render_template(
            "file_viewer_preview.html",
            code=code,
            wholesaler_name=WHOLESALER_NAMES.get(code, code),
            filename=filename,
            tables=[],
            error=str(e),
        )

    return render_template(
        "file_viewer_preview.html",
        code=code,
        wholesaler_name=WHOLESALER_NAMES.get(code, code),
        filename=filename,
        tables=tables,
        error=None,
    )


def _read_xlsx_bytes(data: bytes, max_rows: int = 500):
    import openpyxl
    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active
    rows = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i >= max_rows:
            break
        rows.append([str(c) if c is not None else "" for c in row])
    wb.close()
    return rows
