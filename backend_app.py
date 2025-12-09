import os
import math
from typing import Optional, List, Dict

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path

# ------------------------------------------------------------
# קונפיג – נתיבי פרויקט (יחסיים)
# ------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"

# קובץ ה־SPA (פרונטאנד)
FRONTEND_FILE = BASE_DIR / "frontend_spa.html"

# שם קובץ ה-CSV הקבוע שממנו נטענים הנתונים
FIXED_CSV_NAME = "all_companies_all_yields.csv"

NO_CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}

# ------------------------------------------------------------
# FastAPI
# ------------------------------------------------------------
app = FastAPI(title="מרכיבי תשואה – Dashboard API v2.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def no_cache_middleware(request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# ------------------------------------------------------------
# פונקציות עזר
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

def classify_liquidity_from_category(hebrew_category: Optional[str]) -> Optional[str]:
    """
    כללי הסחירות:
    - אם אפיק השקעה הוא אחד מ: נדל"ן / קרנות השקעה / הלוואות ⇒ "לא סחיר"
    - אחרת ⇒ "סחיר"
    """
    if hebrew_category is None:
        return None
    cat = str(hebrew_category).strip()
    illiquid = {"נדל\"ן", "קרנות השקעה", "הלוואות"}
    return "לא סחיר" if cat in illiquid else "סחיר"

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
    קורא תמיד מקובץ קבוע: all_companies_all_yields.csv בתיקיית output.
    """

    csv_path = OUTPUT_DIR / FIXED_CSV_NAME

    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"לא נמצא קובץ CSV: {csv_path}",
        )

    # קריאה עם קידוד
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    # העמודות שמצופות בקובץ
    # שמרנו על תאימות לאחור: "סחירות" אינה חובה; אם חסרה – נחשב אותה מתוך "אפיק השקעה".
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

    # אם אין עמודת "סחירות" בקובץ – נחשב אותה לפי הכלל
    if "סחירות" not in df.columns:
        df["סחירות"] = df["אפיק השקעה"].apply(classify_liquidity_from_category)
    else:
        # מנרמלים לכתיב אחיד ("סחיר"/"לא סחיר") למקרה שיש וריאציות
        df["סחירות"] = df["סחירות"].apply(
            lambda v: classify_liquidity_from_category(df.loc[df.index[df["סחירות"] == v], "אפיק השקעה"].iloc[0])
            if v not in ("סחיר", "לא סחיר") else v
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
        "ח.פ": "company_id",
        "שנה": "year",
        "רבעון": "quarter",
        "תרומה": "contribution",
        "משקל": "weight",
        "תשואה": "yield",
        "סחירות": "liquidity",
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

    # meta עבור פילטרים ותצוגה
    def unique_sorted(col: str):
        vals = sorted({r[col] for r in rows if r.get(col) is not None})
        return vals

    years = unique_sorted("year")
    meta = {
        "file": FIXED_CSV_NAME,
        "companies": unique_sorted("company_short"),
        "tracks": unique_sorted("track_name"),
        "categories": unique_sorted("category"),
        "saving_types": unique_sorted("saving_type"),
        "fund_types": unique_sorted("fund_type"),
        "liquidity": unique_sorted("liquidity"),  # חדש – ערכי "סחיר"/"לא סחיר"
        "years": years,
        "min_year": years[0] if years else None,
        "max_year": years[-1] if years else None,
        "total_rows": len(rows),
    }

    # החזרת JSON תקין לחלוטין (ללא NaN/INF)
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
