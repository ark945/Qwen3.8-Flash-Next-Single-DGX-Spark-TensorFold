#!/usr/bin/env python3
"""
Stock Data Analysis Engine using DuckDB for Parquet files in /home/blue/stock_data
Provides schema inspection and safe, high-performance SQL query execution.
"""

from __future__ import annotations

import datetime
import glob
import json
import os
import re
from typing import Any, Dict, List, Optional

try:
    import duckdb
except ImportError:
    duckdb = None

STOCK_TABLE_DESCRIPTIONS = {
    "close_price": (
        "每日收盤價/量價 (api_close1_*.parquet)\n"
        "欄位: symbol (股票代號), name (名稱), trade_date (日期 YYYY-MM-DD), market (TWSE/TPEX), "
        "open (開盤價), high (最高價), low (最低價), close (收盤價), change (漲跌), "
        "volume (成交股數), transaction_count (成交筆數), turnover (成交金額), "
        "last_bid_price (買進價), last_ask_price (賣出價)"
    ),
    "margin": (
        "信用交易/融資融券明細 (api_margin_*.parquet)\n"
        "欄位: symbol, name, trade_date, market, margin_buy (融資買進), margin_sell (融資賣出), "
        "margin_cash_repay (現金償還), margin_prev_balance (前日餘額), margin_balance (融資餘額), "
        "margin_limit (融資限額), margin_utilization_pct (融資使用率%), short_buy (融券買進), "
        "short_sell (融券賣出), short_cash_repay (融券現券償還), short_prev_balance, "
        "short_balance (融券餘額), short_limit, short_utilization_pct (融券使用率%), "
        "offset_share (資券互抵), margin_net (融資淨買賣), short_net (融券淨買賣), "
        "short_margin_ratio_pct (券資比%), note"
    ),
    "taifex": (
        "期貨大戶/散戶部位 (api_taifex_*.parquet)\n"
        "欄位: trade_date, foreign_tx_oi (外資台指期留倉), investment_tx_oi (投信留倉), "
        "dealer_tx_oi (自營商留倉), institutional_tx_total_oi (三大法人台指期總留倉), "
        "foreign_mtx_oi (外資小台留倉), investment_mtx_oi (投信小台留倉), "
        "dealer_mtx_oi (自營商小台留倉), institutional_mtx_total_oi (三大法人小台總留倉), "
        "total_tx_oi (全市場台指期留倉), total_mtx_oi (全市場小台留倉), "
        "retail_mtx_net (散戶小台淨部位), retail_mtx_ratio_pct (散戶小台多空比率%), macro_sentiment (情緒警語)"
    ),
    "tdcc": (
        "千張大戶股權分散 (api_tdcc_*.parquet - 每週五更新)\n"
        "欄位: symbol, trade_date, large_shareholder_pct (千張大戶持股比率%), "
        "large_shareholder_count (千張大戶人數), retail_shareholder_pct (散戶持股比率%), "
        "retail_shareholder_count (散戶人數), total_shareholders (總股東人數), total_shares (總發行股數)"
    ),
    "revenue": (
        "全市場月營收 (api_revenue_*.parquet，涵蓋至 2026-09 最新每月營收)\n"
        "欄位: stock_id (或 symbol, 股票代號), stock_name (或 name, 股名), year_month (或 trade_date, 營收年月 YYYY-MM), "
        "report_month (申報年月), market_type (上市/上櫃), rev_current (或 revenue, 當月營收-千元), "
        "rev_last_month (上月營收), rev_last_year (去年同月營收), mom_pct (月增率%), yoy_pct (年增率%), "
        "rev_accumulated (或 cum_revenue, 當年累計營收), rev_accumulated_last_year (去年同期累計營收), "
        "yoy_accumulated_pct (累計年增率%), remark (備註說明)\n"
        "提示: 支援 stock_id 與 symbol 雙向查詢，營收年月為 'YYYY-MM'（如 '2026-09'）。"
    ),
    "broker_trade": (
        "券商分點買賣超 (api_absr1_*.parquet，已自動關聯 broker_name_map.json 券商中文名稱)\n"
        "欄位: symbol, trade_date, broker_id (券商分點代碼), broker_name (券商分點中文名稱，如台灣摩根士丹利、元大、富邦、香港上海匯豐等), "
        "buy_vol (買進股數), sell_vol (賣出股數), net_vol (淨買超股數), "
        "buy_amt (買進金額), sell_amt (賣出金額), net_amt (淨買超金額), "
        "buy_avg_price (買均價), sell_avg_price (賣均價), turnover, market_share\n"
        "提示: 資料表已內建 broker_name 欄位，產出分析與表格時請務必帶上 broker_name 券商分點中文名稱！"
    ),
    "treasury_stock": (
        "庫藏股最新全市場快照 (treasury_stocks_latest.parquet)\n"
        "說明: 即時用途，全市場進行中與歷史庫藏股最新統計資料。\n"
        "欄位 (中英雙向支援): symbol (或 代碼, 股票代號), name (或 名稱, 股名), "
        "board_date (或 董事會日期, 格式如 '115/09/30'), start_date (或 庫藏股開始), end_date (或 庫藏股結束), "
        "target_shares (或 預計買回股數), bought_shares (或 已買回股數), "
        "price_low (或 區間～低, 買回區間下限元), price_high (或 區間～高, 買回區間上限元), "
        "is_finished (或 執行完畢, Y/N), reason (或 本次未執行完畢之原因)"
    ),
    "treasury_history": (
        "庫藏股每日歷史時點存檔 (treasury_stocks_YYYYMMDD.parquet)\n"
        "說明: 回溯用途，每日歷史時點存檔，可用於比對每日買回進度、推估公司護盤時點。\n"
        "欄位 (中英雙向支援): trade_date (或 snapshot_date, 存檔日期 YYYY-MM-DD), "
        "symbol (或 代碼), name (或 名稱), board_date (董事會日期), start_date (庫藏股開始), end_date (庫藏股結束), "
        "target_shares (預計買回股數), bought_shares (已買回股數), price_low (區間～低), price_high (區間～高), "
        "is_finished (執行完畢), reason (本次未執行完畢之原因)"
    )
}

def _serialize_cell(val: Any) -> Any:
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    return val

class StockEngine:
    def __init__(self, data_dir: str = "/home/blue/stock_data"):
        self.data_dir = data_dir
        self.conn = None
        self.views_registered: Dict[str, bool] = {}
        self.latest_dates: Dict[str, str] = {}
        self.broker_names: Dict[str, str] = {}
        self.init_database()

    def _load_broker_names(self):
        """Load broker_name_map.json from data_dir or known fallback paths."""
        candidates = [
            os.path.join(self.data_dir, "broker_name_map.json"),
            "/home/blue/stock_data/broker_name_map.json",
            "/home/blue/stock_data_downloader/output/broker_name_map.json",
            "/home/blue/stock_data_downloader/broker_name_map.json",
        ]
        loaded_path = None
        for path in candidates:
            if os.path.isfile(path):
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        self.broker_names = json.load(f)
                    loaded_path = path
                    break
                except Exception as e:
                    print(f"[StockEngine] 讀取 {path} 失敗: {e}")

        if loaded_path:
            print(f"[StockEngine] 成功載入券商分點對照表: {loaded_path} (共 {len(self.broker_names)} 筆)")
        else:
            print("[StockEngine] 未找到 broker_name_map.json，將無法顯示分點中文名稱。")

        if self.conn is not None:
            try:
                self.conn.execute("CREATE OR REPLACE TABLE broker_names (broker_id VARCHAR PRIMARY KEY, broker_name VARCHAR)")
                if self.broker_names:
                    self.conn.executemany("INSERT INTO broker_names VALUES (?, ?)", list(self.broker_names.items()))
                print(f"[StockEngine] DuckDB broker_names 字典表建立完成")
            except Exception as e:
                print(f"[StockEngine] 建立 broker_names 字典表失敗: {e}")

    def init_database(self):
        if duckdb is None:
            print("[StockEngine] DuckDB 模組未安裝，無法初始化資料庫！", flush=True)
            return

        try:
            self.conn = duckdb.connect(":memory:")
            self._load_broker_names()
            self._register_views()
            self._cache_latest_dates()
            print("[StockEngine] DuckDB 股票資料表視圖載入完成！", flush=True)
        except Exception as e:
            print(f"[StockEngine] DuckDB 初始化失敗: {e}", flush=True)

    def _register_views(self):
        table_mappings = [
            ("close_price", "api_close1_*.parquet"),
            ("margin", "api_margin_*.parquet"),
            ("taifex", "api_taifex_*.parquet"),
            ("tdcc", "api_tdcc_*.parquet"),
            ("revenue", "api_revenue_????-??.parquet"),
            ("broker_trade", "api_absr1_*.parquet"),
            ("treasury_stock", "treasury_stocks_latest.parquet"),
            ("treasury_history", "treasury_stocks_[0-9]*.parquet"),
        ]

        for table_name, pattern in table_mappings:
            matching_files = glob.glob(os.path.join(self.data_dir, pattern))
            if matching_files:
                glob_path = os.path.join(self.data_dir, pattern)
                try:
                    if table_name == "broker_trade" and self.broker_names:
                        view_sql = f"""
                        CREATE OR REPLACE VIEW broker_trade AS 
                        SELECT 
                            b.symbol,
                            b.trade_date,
                            b.broker_id,
                            COALESCE(m.broker_name, '未知券商') AS broker_name,
                            b.buy_vol,
                            b.sell_vol,
                            b.net_vol,
                            b.buy_amt,
                            b.sell_amt,
                            b.net_amt,
                            b.buy_avg_price,
                            b.sell_avg_price,
                            b.turnover,
                            b.market_share
                        FROM read_parquet('{glob_path}') b
                        LEFT JOIN broker_names m ON b.broker_id = m.broker_id
                        """
                    elif table_name == "revenue":
                        view_sql = f"""
                        CREATE OR REPLACE VIEW revenue AS 
                        SELECT 
                            stock_id AS symbol,
                            stock_name AS name,
                            year_month AS trade_date,
                            rev_current AS revenue,
                            rev_accumulated AS cum_revenue,
                            stock_id,
                            stock_name,
                            year_month,
                            report_month,
                            market_type,
                            rev_current,
                            rev_last_month,
                            rev_last_year,
                            mom_pct,
                            yoy_pct,
                            rev_accumulated,
                            rev_accumulated_last_year,
                            yoy_accumulated_pct,
                            remark
                        FROM read_parquet('{glob_path}')
                        """
                    elif table_name == "treasury_stock":
                        view_sql = f"""
                        CREATE OR REPLACE VIEW treasury_stock AS 
                        SELECT 
                            代碼 AS symbol,
                            名稱 AS name,
                            董事會日期 AS board_date,
                            庫藏股開始 AS start_date,
                            庫藏股結束 AS end_date,
                            預計買回股數 AS target_shares,
                            已買回股數 AS bought_shares,
                            "區間～低" AS price_low,
                            "區間～高" AS price_high,
                            執行完畢 AS is_finished,
                            本次未執行完畢之原因 AS reason,
                            代碼,
                            名稱,
                            董事會日期,
                            庫藏股開始,
                            庫藏股結束,
                            預計買回股數,
                            已買回股數,
                            "區間～低",
                            "區間～高",
                            執行完畢,
                            本次未執行完畢之原因
                        FROM read_parquet('{glob_path}')
                        """
                    elif table_name == "treasury_history":
                        view_sql = f"""
                        CREATE OR REPLACE VIEW treasury_history AS 
                        SELECT 
                            strptime(regexp_extract(filename, 'treasury_stocks_(\\d{{8}})\\.parquet', 1), '%Y%m%d')::DATE::VARCHAR AS trade_date,
                            strptime(regexp_extract(filename, 'treasury_stocks_(\\d{{8}})\\.parquet', 1), '%Y%m%d')::DATE::VARCHAR AS snapshot_date,
                            代碼 AS symbol,
                            名稱 AS name,
                            董事會日期 AS board_date,
                            庫藏股開始 AS start_date,
                            庫藏股結束 AS end_date,
                            預計買回股數 AS target_shares,
                            已買回股數 AS bought_shares,
                            "區間～低" AS price_low,
                            "區間～高" AS price_high,
                            執行完畢 AS is_finished,
                            本次未執行完畢之原因 AS reason,
                            代碼,
                            名稱,
                            董事會日期,
                            庫藏股開始,
                            庫藏股結束,
                            預計買回股數,
                            已買回股數,
                            "區間～低",
                            "區間～高",
                            執行完畢,
                            本次未執行完畢之原因
                        FROM read_parquet('{glob_path}', filename=true)
                        """
                    else:
                        view_sql = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{glob_path}')"
                    self.conn.execute(view_sql)
                    self.views_registered[table_name] = True
                    print(f"[StockEngine] 已掛載視圖: {table_name} ({len(matching_files)} 檔案)")
                except Exception as e:
                    print(f"[StockEngine] 掛載視圖 {table_name} 失敗: {e}")
                    self.views_registered[table_name] = False
            else:
                self.views_registered[table_name] = False
                if table_name == "revenue":
                    self.conn.execute(
                        "CREATE TABLE IF NOT EXISTS revenue ("
                        "symbol VARCHAR, name VARCHAR, trade_date VARCHAR, "
                        "revenue DOUBLE, mom_pct DOUBLE, yoy_pct DOUBLE, cum_revenue DOUBLE, "
                        "stock_id VARCHAR, stock_name VARCHAR, year_month VARCHAR, rev_current DOUBLE)"
                    )
                    self.views_registered[table_name] = True
                    print(f"[StockEngine] 建立空表結構: {table_name} (等待檔案寫入)")

    def _cache_latest_dates(self):
        """Query and cache the latest trade_date for each table for quick context prompt."""
        for table in ["close_price", "margin", "taifex", "tdcc", "broker_trade", "revenue", "treasury_stock", "treasury_history"]:
            if self.views_registered.get(table):
                try:
                    if table == "treasury_stock":
                        res = self.conn.execute("SELECT CAST(MAX(board_date) AS VARCHAR) FROM treasury_stock").fetchone()
                    else:
                        res = self.conn.execute(f"SELECT CAST(MAX(trade_date) AS VARCHAR) FROM {table}").fetchone()
                    if res and res[0]:
                        self.latest_dates[table] = str(res[0])
                except Exception:
                    pass

    def get_summary_prompt(self) -> str:
        """Returns a concise description of available tables and latest dates for LLM system prompt."""
        lines = ["[本地台股 DuckDB 資料庫現況]"]
        for table, desc in STOCK_TABLE_DESCRIPTIONS.items():
            if self.views_registered.get(table):
                latest = self.latest_dates.get(table, "無或為空")
                lines.append(f"• 表名 `{table}`: (最新日期: {latest})")
                lines.append(f"  {desc}")
        lines.append("提示: 查詢時請用標準 DuckDB SQL。若查詢個股，symbol 為字串（如 '2330'）。")
        return "\n".join(lines)

    def execute_query(self, sql: str, max_rows: int = 100) -> str:
        """Executes a SQL query safely and formats the result."""
        if self.conn is None:
            return "[錯誤: DuckDB 引擎未就緒]"

        cleaned_sql = sql.strip().rstrip(";")

        # Security check: Read-only verification
        forbidden_patterns = [
            r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b",
            r"\bALTER\b", r"\bCREATE\b", r"\bCOPY\b", r"\bATTACH\b",
            r"\bDETACH\b", r"\bPRAGMA\b", r"\bINSTALL\b", r"\bLOAD\b"
        ]
        for pattern in forbidden_patterns:
            if re.search(pattern, cleaned_sql, re.IGNORECASE):
                return f"[安全限制: 僅允許唯讀 SELECT 查詢，禁止執行: {pattern.replace(r'\\b', '')}]"

        # Check if LIMIT exists, if not cap it to max_rows
        if not re.search(r"\bLIMIT\s+\d+", cleaned_sql, re.IGNORECASE):
            wrapped_sql = f"{cleaned_sql} LIMIT {max_rows}"
        else:
            wrapped_sql = cleaned_sql

        try:
            cursor = self.conn.execute(wrapped_sql)
            if cursor.description is None:
                return "查詢完成，無回傳資料。"

            cols = [desc[0] for desc in cursor.description]
            raw_rows = cursor.fetchall()

            if not raw_rows:
                return "查詢結果: 0 筆資料 (查無相符結果)。"

            rows = [[_serialize_cell(c) for c in r] for r in raw_rows]
            records = [dict(zip(cols, r)) for r in rows]

            # 若查詢結果包含券商分點代碼 broker_id，自動帶上中文名稱 broker_name
            if self.broker_names:
                for rec in records:
                    if "broker_id" in rec and rec["broker_id"] is not None:
                        bid = str(rec["broker_id"]).strip()
                        if "broker_name" not in rec or not rec["broker_name"] or rec["broker_name"] == "未知券商":
                            rec["broker_name"] = self.broker_names.get(bid, "未知券商")

            result_json = json.dumps(records, ensure_ascii=False)
            if len(raw_rows) > max_rows:
                result_json += f"\n(已自動截斷至最多 {max_rows} 筆)"
            return result_json

        except Exception as e:
            return f"[DuckDB SQL 執行錯誤: {e}]"

    def get_schema(self, table_name: str) -> str:
        """Get schema and sample columns for a specific table."""
        if table_name not in STOCK_TABLE_DESCRIPTIONS:
            return f"無此資料表 `{table_name}`。可用資料表: {list(STOCK_TABLE_DESCRIPTIONS.keys())}"

        info = STOCK_TABLE_DESCRIPTIONS.get(table_name, "")
        latest = self.latest_dates.get(table_name, "未知")
        return f"資料表: {table_name} (最新日期: {latest})\n{info}"
