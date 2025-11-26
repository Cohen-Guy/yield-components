import os
import math
from typing import Optional, List, Dict

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path

# ------------------------------------------------------------
# קונפיג – נתיבי פרויקט
# ------------------------------------------------------------
# ------------------------------------------------------------
# קונפיג – נתיבי פרויקט (יחסיים)
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"

# קובץ ה־SPA
FRONTEND_FILE = BASE_DIR / "frontend_spa.html"


NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}


# ------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------
app = FastAPI(title="מרכיבי תשואה – Dashboard API v2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ------------------------------------------------------------
# פונקציה לאיתור קובץ CSV האחרון
# ------------------------------------------------------------
def find_latest_csv(folder) -> Optional[str]:
    folder = str(folder)
    if not os.path.isdir(folder):
        return None

    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith(".csv")
    ]
    if not files:
        return None

    return max(files, key=os.path.getmtime)



# ------------------------------------------------------------
# פונקציה שמנקה NaN / INF כדי למנוע תקיעת JSONResponse
# ------------------------------------------------------------
def clean_nan_inf(value):
    if value is None:
        return None
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value == math.inf or value == -math.inf:
            return None
    return value


def clean_row(row: dict) -> dict:
    return {k: clean_nan_inf(v) for k, v in row.items()}


# ------------------------------------------------------------
# ROOT – מגיש את ה-SPA
# ------------------------------------------------------------
@app.get("/", response_class=FileResponse)
def root():
    if not os.path.exists(FRONTEND_FILE):
        raise HTTPException(
            status_code=500,
            detail=f"לא נמצא קובץ SPA: {FRONTEND_FILE}",
        )
    return FileResponse(FRONTEND_FILE, headers=NO_CACHE_HEADERS)


# ------------------------------------------------------------
# /api/data – טוען את הנתונים, מנקה אותם ומחזיר JSON תקין
# ------------------------------------------------------------
@app.get("/api/data")
def api_data():
    """
    מחזיר את כל הרשומות + meta בצורה אחידה.
    """

    csv_path = find_latest_csv(OUTPUT_DIR)
    if not csv_path:
        raise HTTPException(status_code=404, detail="לא נמצא קובץ CSV בתיקייה output")

    # קריאה עם קידוד
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    # העמודות שמצופות בקובץ
    required_cols = [
        "אפיק השקעה",
        "מס מסלול",
        "שם מסלול אחיד",
        "שם חברה",
        "חברה מקוצר",
        "סוג חיסכון",
        "סוג קופה",
        "סוג מסלול",
        "ח.פ",
        "שנה",
        "רבעון",
        "תרומה",
        "משקל",
        "תשואה",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"עמודות חסרות בקובץ: {', '.join(missing)}",
        )

    # מיפוי שמות לעברית → אנגלית
    rename_map = {
        "אפיק השקעה": "category",
        "מס מסלול": "track_id",
        "שם מסלול אחיד": "track_name",
        "שם חברה": "company",
        "חברה מקוצר": "company_short",
        "סוג חיסכון": "saving_type",
        "סוג קופה": "fund_type",
        "סוג מסלול": "track_type",
        "ח.פ": "company_id",
        "שנה": "year",
        "רבעון": "quarter",
        "תרומה": "contribution",
        "משקל": "weight",
        "תשואה": "yield",
    }

    df = df.rename(columns=rename_map)

    # המרות טיפוסים
    for col in ["year", "quarter"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    for col in ["contribution", "weight", "yield"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # המרת INF → NaN
    df = df.replace([math.inf, -math.inf], pd.NA)

    # המרת NaN ל-None
    df = df.where(df.notnull(), None)

    # הפיכת הרשומות למילונים
    raw_rows = df.to_dict(orient="records")

    # ניקוי NaN/INF בכל שורה
    rows: List[Dict] = [clean_row(r) for r in raw_rows]

    # meta עבור פילטרים
    def unique_sorted(col: str):
        vals = sorted({r[col] for r in rows if r.get(col) is not None})
        return vals

    years = unique_sorted("year")
    meta = {
        "file": os.path.basename(csv_path),
        "companies": unique_sorted("company_short"),
        "tracks": unique_sorted("track_name"),
        "categories": unique_sorted("category"),
        "saving_types": unique_sorted("saving_type"),
        "fund_types": unique_sorted("fund_type"),
        "years": years,
        "min_year": years[0] if years else None,
        "max_year": years[-1] if years else None,
        "total_rows": len(rows),
    }

    # החזרת JSON תקין לחלוטין (ללא NaN)
    return JSONResponse(
        {
            "status": "ok",
            "rows": rows,
            "meta": meta,
        },
        headers=NO_CACHE_HEADERS,
    )


# ------------------------------------------------------------
# MAIN – הרצה לוקאלית
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("backend_app:app", host="127.0.0.1", port=8010, reload=True)
