import os
import io
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse
import google.generativeai as genai
import pymssql
import openpyxl
from dotenv import load_dotenv

load_dotenv()

# ── DB 設定 ──────────────────────────────────────────────
DB_SERVER   = os.getenv("DB_SERVER",   "163.17.141.61,8000")
DB_NAME     = os.getenv("DB_NAME",     "gemio")
DB_USER     = os.getenv("DB_USER",     "drcas")
DB_PASS     = os.getenv("DB_PASSWORD", "")

# ── 快取 ─────────────────────────────────────────────────
_schema_cache: dict  = {}
_tables_cache: list  = []
_last_results: dict  = {"columns": [], "rows": [], "sql": ""}

# ── DB 連線 ───────────────────────────────────────────────
def get_connection():
    # DB_SERVER 格式: "163.17.141.61,8000" → host/port 分開
    host, port = DB_SERVER.replace(" ", "").split(",")
    return pymssql.connect(
        host=host,
        port=int(port),
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        login_timeout=15,
        as_dict=False,
    )

# ── 載入 Schema ───────────────────────────────────────────
def load_schema():
    global _schema_cache, _tables_cache
    try:
        conn   = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM   INFORMATION_SCHEMA.TABLES
            WHERE  TABLE_CATALOG = %s AND TABLE_TYPE IN ('BASE TABLE','VIEW')
            ORDER  BY TABLE_TYPE DESC, TABLE_NAME
        """, (DB_NAME,))
        tables        = cursor.fetchall()
        _tables_cache = [{"name": t[0], "type": t[1]} for t in tables]

        schema = {}
        for tbl in tables:
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM   INFORMATION_SCHEMA.COLUMNS
                WHERE  TABLE_CATALOG = %s AND TABLE_NAME = %s
                ORDER  BY ORDINAL_POSITION
            """, (DB_NAME, tbl[0]))
            schema[tbl[0]] = [
                {"name": c[0], "type": c[1]} for c in cursor.fetchall()
            ]
        _schema_cache = schema
        conn.close()
        print(f"[schema] 載入 {len(_tables_cache)} 張資料表/檢視表")
    except Exception as e:
        print(f"[schema] 載入失敗: {e}")

def schema_text() -> str:
    lines = []
    for tbl, cols in _schema_cache.items():
        col_defs = ", ".join(f"{c['name']}({c['type']})" for c in cols)
        lines.append(f"{tbl}: {col_defs}")
    return "\n".join(lines)

# 啟動時載入
load_schema()

# ── FastAPI ───────────────────────────────────────────────
app       = FastAPI()
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "tables":  _tables_cache,
    })


@app.post("/query", response_class=HTMLResponse)
async def query(request: Request, question: str = Form(...)):
    global _last_results
    sql   = ""
    error = None
    columns: list = []
    rows:    list = []
    totals:  dict = {}

    try:
        # ── 1. Gemini → SQL ──────────────────────────────
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        model = genai.GenerativeModel(
            model_name="gemini-2.5-flash",
            system_instruction=f"""你是一個 SQL Server T-SQL 查詢助手。
資料庫名稱: {DB_NAME}（SQL Server）
Schema（資料表/欄位）:
{schema_text()}

規則:
1. 只輸出可直接執行的 SELECT T-SQL，不加說明文字、不加 Markdown 標記
2. 預設加上 TOP 1000 限制
3. 日期格式使用 CONVERT(varchar, column, 111)
4. 若無法判斷，輸出: SELECT '無法理解此查詢，請重新描述' AS 訊息""",
        )
        resp = model.generate_content(question)
        sql = resp.text.strip()
        # 移除可能的 markdown code block
        if "```" in sql:
            sql = sql.split("```")[1]
            if sql.lower().startswith("sql"):
                sql = sql[3:]
            sql = sql.strip()

        # ── 2. 執行 SQL ──────────────────────────────────
        conn   = get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        raw_rows = cursor.fetchall()
        conn.close()

        rows = [[("" if v is None else str(v)) for v in row] for row in raw_rows]

        # ── 3. 計算數字欄位合計 ───────────────────────────
        for i, col in enumerate(columns):
            nums = []
            for row in rows:
                try:
                    nums.append(float(row[i].replace(",", "")))
                except (ValueError, AttributeError):
                    pass
            if len(nums) == len(rows) and len(rows) > 0:
                totals[i] = f"{sum(nums):,.2f}"

        _last_results = {"columns": columns, "rows": rows, "sql": sql}

    except Exception as e:
        error = str(e)

    return templates.TemplateResponse("results.html", {
        "request": request,
        "columns": columns,
        "rows":    rows,
        "totals":  totals,
        "sql":     sql,
        "error":   error,
    })


@app.get("/export")
async def export_excel():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "查詢結果"
    cols = _last_results["columns"]
    rows = _last_results["rows"]
    if cols:
        ws.append(cols)
        for row in rows:
            ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=query_result.xlsx"},
    )


@app.post("/refresh-schema")
async def refresh_schema():
    load_schema()
    return {"status": "ok", "count": len(_tables_cache)}
