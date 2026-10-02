#!/usr/bin/env python3
"""
TensorFold Smart Gateway with DuckDB Stock Analysis & Native Tool Calling
Listens on PORT (default 1234).
Transparently provides OpenAI Function/Tool Calling for:
1. `query_stock_data`: Local DuckDB SQL analytics for Taiwan stock Parquet files in /home/blue/stock_data/
2. `get_stock_schema`: Schema and column dictionary for stock tables
3. `web_search`: Real-time web search via local SearXNG (default http://127.0.0.1:8080)
Proxies all non-chat requests directly to upstream TensorFold (default 127.0.0.1:1235).
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# Ensure local imports work regardless of CWD
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from stock_engine import StockEngine

# Gateway Tool Definitions for OpenAI API
GATEWAY_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_stock_data",
            "description": (
                "執行 DuckDB SQL 查詢台股本地量化資料庫 (/home/blue/stock_data/)。"
                "支援資料表：\n"
                "• close_price: 每日收盤價/量價 (symbol, name, trade_date, open, high, low, close, change, volume, turnover 等)\n"
                "• margin: 信用交易/融資融券 (margin_balance, margin_net, short_balance, short_net, short_margin_ratio_pct 等)\n"
                "• taifex: 期貨大戶/散戶留倉 (foreign_tx_oi, investment_tx_oi, retail_mtx_net, retail_mtx_ratio_pct, macro_sentiment 等)\n"
                "• tdcc: 千張大戶股權分散 (large_shareholder_pct, retail_shareholder_pct, total_shareholders 等)\n"
                "• revenue: 全市場月營收 (最新至 2026-09。欄位: stock_id/symbol 股票代號, stock_name/name 股名, year_month/trade_date 營收年月如 '2026-09', rev_current/revenue 當月營收千元, mom_pct 月增率%, yoy_pct 年增率%, rev_accumulated/cum_revenue 累計營收, yoy_accumulated_pct 等)\n"
                "• broker_trade: 券商分點買賣超 (symbol, trade_date, broker_id, broker_name 券商分點中文名稱如台灣摩根士丹利/富邦/元大/香港上海匯豐, net_vol, net_amt 等)\n"
                "• treasury_stock: 庫藏股最新全市場快照 (treasury_stocks_latest.parquet。欄位: symbol/代碼, name/名稱, board_date/董事會日期, start_date/庫藏股開始, end_date/庫藏股結束, target_shares/預計買回股數, bought_shares/已買回股數, price_low/區間～低, price_high/區間～高, is_finished/執行完畢 等)\n"
                "• treasury_history: 庫藏股歷史每日時點存檔 (treasury_stocks_YYYYMMDD.parquet。欄位: trade_date/snapshot_date 存檔日期, symbol/代碼, name/名稱, board_date, start_date, end_date, target_shares, bought_shares, price_low, price_high, is_finished, reason 等，可用於比對每日買回進度)\n"
                "支援所有標準 DuckDB SQL 語法與視窗分析函數。"
            ),
            "parameters": {
                "type": "object",
                "required": ["sql", "reasoning"],
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "標準 DuckDB SQL 查詢語法。例如: 'SELECT symbol, name, trade_date, close, volume FROM close_price WHERE symbol = \\'2330\\' ORDER BY trade_date DESC LIMIT 5;'"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "執行此查詢的原因或目的簡述。"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_stock_schema",
            "description": "查詢台股資料庫中指定資料表的欄位定義與最新交易日，避免欄位名稱寫錯。",
            "parameters": {
                "type": "object",
                "required": ["table_name"],
                "properties": {
                    "table_name": {
                        "type": "string",
                        "enum": ["close_price", "margin", "taifex", "tdcc", "revenue", "broker_trade", "treasury_stock", "treasury_history"],
                        "description": "欲查詢結構的資料表名稱。"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "當需要查詢台股最新盤勢即時新聞、重大財經事件、產業趨勢或網路上其他即時資訊時調用。",
            "parameters": {
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "搜尋關鍵字詞。"
                    }
                }
            }
        }
    }
]

def search_searxng(query: str, searxng_url: str, count: int = 3, timeout: float = 6.0) -> list[dict]:
    """Query SearXNG JSON API."""
    params = urllib.parse.urlencode({"q": query, "format": "json"})
    target = f"{searxng_url.rstrip('/')}/search?{params}"
    try:
        req = urllib.request.Request(
            target,
            headers={"User-Agent": "TensorFold-Gateway/2.0"}
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            results = data.get("results", [])
            output = []
            for r in results[:count]:
                output.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "content": r.get("content", "")
                })
            return output
    except Exception as e:
        print(f"[Gateway] SearXNG 搜尋連線異常 ({target}): {e}", flush=True)
        return [{"error": f"SearXNG 搜尋失敗: {e}"}]

def clean_sql_string(sql: str) -> str:
    """Clean common LLM SQL syntax anomalies such as repeated OR/AND or truncated quotes."""
    sql = re.sub(r"\bOR\s+OR\b", "OR", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\bAND\s+AND\b", "AND", sql, flags=re.IGNORECASE)
    # Fix trailing incomplete single quote or broken clause
    if sql.count("'") % 2 != 0:
        last_quote = sql.rfind("'")
        prev_or = sql.rfind(" OR ", 0, last_quote)
        if prev_or != -1:
            sql = sql[:prev_or]
        else:
            sql = sql + "'"
    sql = re.sub(r"\s+OR\s*$", "", sql, flags=re.IGNORECASE)
    return sql.strip()

def parse_raw_qwen_tool_calls(content: str) -> list[dict]:
    """Parse raw Qwen <tool_call> tags when upstream did not parse them into JSON tool_calls."""
    calls = []
    pattern = re.compile(r"<tool_call>([\s\S]*?)(?:</tool_call>|$)", re.IGNORECASE)
    for match in pattern.finditer(content):
        block = match.group(1).strip()
        if not block:
            continue
        # Case 1: JSON payload inside <tool_call>
        if block.startswith("{") and block.endswith("}"):
            try:
                data = json.loads(block)
                name = data.get("name")
                args = data.get("arguments", {})
                if name:
                    calls.append({
                        "id": f"call_raw_{int(time.time()*1000)}",
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": json.dumps(args, ensure_ascii=False) if isinstance(args, dict) else str(args)
                        }
                    })
                    continue
            except Exception:
                pass

        # Case 2: XML-like format <function=...> <parameter=...> ...
        fn_match = re.search(r"<function=([a-zA-Z0-9_]+)>", block)
        if fn_match:
            fn_name = fn_match.group(1)
            param_match = re.search(r"<parameter=([a-zA-Z0-9_]+)>\s*([\s\S]*)", block)
            if param_match:
                param_name = param_match.group(1)
                param_val = param_match.group(2).strip()
                param_val = re.sub(r"</parameter>.*", "", param_val, flags=re.IGNORECASE).strip()
                args = {param_name: param_val}
            else:
                rest = block[fn_match.end():].strip()
                args = {"sql": rest} if fn_name == "query_stock_data" else {"query": rest}

            calls.append({
                "id": f"call_raw_{int(time.time()*1000)}",
                "type": "function",
                "function": {
                    "name": fn_name,
                    "arguments": json.dumps(args, ensure_ascii=False)
                }
            })
    return calls

class GatewayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        # Silence default request logging to keep output clean
        pass

    def _proxy(self, method: str, body: bytes | None = None):
        """Pass-through proxy to upstream TensorFold for non-chat endpoints."""
        upstream_netloc = self.server.upstream_netloc
        path = self.path

        try:
            conn = http.client.HTTPConnection(upstream_netloc[0], upstream_netloc[1], timeout=300)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length")}
            if body is not None:
                headers["Content-Length"] = str(len(body))

            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()

            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() not in ("transfer-encoding",):
                    self.send_header(k, v)
            self.end_headers()

            while chunk := resp.read(8192):
                self.wfile.write(chunk)
                self.wfile.flush()
            conn.close()
        except Exception as e:
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            err = {"error": {"message": f"Gateway 無法連線至 TensorFold ({upstream_netloc}): {e}", "type": "gateway_error"}}
            self.wfile.write(json.dumps(err).encode("utf-8"))

    def do_GET(self):
        self._proxy("GET")

    def do_POST(self):
        # Only intercept /v1/chat/completions (or /chat/completions)
        if not self.path.rstrip("/").endswith("/chat/completions"):
            self._proxy("POST")
            return

        length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(length) if length > 0 else b"{}"

        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except Exception:
            self._proxy("POST", body=body_bytes)
            return

        messages = payload.get("messages", [])
        if not messages or not isinstance(messages, list):
            self._proxy("POST", body=body_bytes)
            return

        # Execute Autonomous Tool-Calling Loop
        self.handle_chat_completions(payload)

    def _call_upstream_non_stream(self, payload: dict) -> dict | None:
        """Call upstream TensorFold non-streaming to inspect tool calls."""
        upstream_netloc = self.server.upstream_netloc
        path = self.path
        body = dict(payload)
        body["stream"] = False

        data = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(data))
        }

        try:
            conn = http.client.HTTPConnection(upstream_netloc[0], upstream_netloc[1], timeout=300)
            conn.request("POST", path, body=data, headers=headers)
            resp = conn.getresponse()
            resp_body = resp.read()
            conn.close()

            if resp.status != 200:
                print(f"[Gateway] Upstream 返回 HTTP {resp.status}: {resp_body.decode('utf-8', 'ignore')[:300]}", flush=True)
                return None

            return json.loads(resp_body.decode("utf-8"))
        except Exception as e:
            print(f"[Gateway] Upstream 請求異常: {e}", flush=True)
            return None

    def _pipe_upstream_stream(self, payload: dict):
        """Invoke upstream TensorFold with stream=True and pipe SSE directly to client."""
        upstream_netloc = self.server.upstream_netloc
        path = self.path
        body = dict(payload)
        body["stream"] = True

        data = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(data))
        }

        try:
            conn = http.client.HTTPConnection(upstream_netloc[0], upstream_netloc[1], timeout=300)
            conn.request("POST", path, body=data, headers=headers)
            resp = conn.getresponse()

            self.send_response(resp.status)
            for k, v in resp.getheaders():
                if k.lower() not in ("transfer-encoding", "connection", "content-length"):
                    self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()

            try:
                while chunk := resp.read(4096):
                    self.wfile.write(chunk)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            conn.close()
            self.close_connection = True
        except Exception as e:
            print(f"[Gateway] 串流轉發異常: {e}", flush=True)
            self.close_connection = True

    def _send_json_response(self, status: int, data: dict):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        self.close_connection = True

    def _send_synthesized_sse(self, message: dict, model: str, response_id: str):
        """Synthesize standard SSE stream for client with guaranteed finish_reason 'stop'."""
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()

        created_ts = int(time.time())
        if not response_id:
            response_id = f"chatcmpl-{int(time.time()*1000)}"
        if not model:
            model = "huihui-qwen3.8-27b-abliterated"

        # Delta 1: Role initialization
        chunk_role = {
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": created_ts,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"role": "assistant"},
                "finish_reason": None
            }]
        }
        try:
            self.wfile.write(f"data: {json.dumps(chunk_role, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True
            return

        # Delta 2: Reasoning content if present
        reasoning = message.get("reasoning_content")
        if reasoning:
            chunk_reason = {
                "id": response_id,
                "object": "chat.completion.chunk",
                "created": created_ts,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {"reasoning_content": reasoning},
                    "finish_reason": None
                }]
            }
            try:
                self.wfile.write(f"data: {json.dumps(chunk_reason, ensure_ascii=False)}\n\n".encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                self.close_connection = True
                return

        # Delta 3: Content in streaming slices
        content = message.get("content") or ""
        if content:
            step = 120
            for i in range(0, len(content), step):
                slice_text = content[i:i+step]
                chunk_slice = {
                    "id": response_id,
                    "object": "chat.completion.chunk",
                    "created": created_ts,
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "delta": {"content": slice_text},
                        "finish_reason": None
                    }]
                }
                try:
                    self.wfile.write(f"data: {json.dumps(chunk_slice, ensure_ascii=False)}\n\n".encode("utf-8"))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    self.close_connection = True
                    return
                time.sleep(0.005)

        # Delta 4: Guaranteed Finish Reason - MUST NEVER BE NULL
        # Cline / Vercel AI SDK throws 'Response stream ended without a finish reason' if this is null or missing!
        raw_finish = message.get("finish_reason")
        finish_reason = raw_finish if (raw_finish and raw_finish != "null") else "stop"

        chunk_end = {
            "id": response_id,
            "object": "chat.completion.chunk",
            "created": created_ts,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": finish_reason
            }]
        }
        try:
            self.wfile.write(f"data: {json.dumps(chunk_end, ensure_ascii=False)}\n\n".encode("utf-8"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        self.close_connection = True

    def handle_chat_completions(self, payload: dict):
        client_wants_stream = bool(payload.get("stream", False))
        messages = payload.get("messages", [])

        # 強制注入頂級台股量化指令，要求模型嚴格使用本地 DuckDB 資料
        stock_summary = self.server.stock_engine.get_summary_prompt()
        system_instruction = (
            "【最高指令：強制優先使用本地 DuckDB 量化資料庫】\n"
            "你是頂尖台股量化分析專家。所有台股量價、融資融券、期貨法人留倉、千張大戶籌碼、營收與券商分點明細，"
            "已全部完整建立在伺服器本地量化資料庫 (/home/blue/stock_data/)。\n"
            "規則：\n"
            "1. 當使用者詢問任何股票行情、券商分點買賣超、外資籌碼、融資變化、期貨部位時，【必須強制調用】本地工具 `query_stock_data` 執行 DuckDB SQL 查詢！\n"
            "2. 嚴禁臆測、嚴禁回答無法查詢或缺乏工具，所有數據皆在本地表中。\n"
            "3. 支援資料表：close_price, margin, taifex, tdcc, revenue, broker_trade, treasury_stock (庫藏股最新快照), treasury_history (庫藏股每日歷史時點存檔)。\n"
            "4. 券商分點資料表 broker_trade 已自動關聯 broker_name_map.json，內建 `broker_name`（券商中文名稱，例如「台灣摩根士丹利」、「富邦」、「元大」、「香港上海匯豐」等）。產出分點資料、買賣超排行表格與籌碼分析時，【必須同時標註 broker_name 券商分點中文名稱】，絕對不可只顯示四碼代號！\n"
            "5. 【SQL 編寫效率規範（嚴禁窮舉大量 OR）】：\n"
            "   - 嚴禁在 WHERE 中連續手寫數十或數百個 `OR broker_id LIKE '...'`，這會導致 SQL 過長截斷並造成語法錯誤！\n"
            "   - 範圍查詢請用：`WHERE broker_id BETWEEN '1110' AND '1999'`\n"
            "   - 多個特定代號請用：`WHERE broker_id IN ('1480', '1440', '1650', '1560', '1590')`\n"
            "   - 模糊查詢券商名稱請善用內建名稱：`WHERE broker_name LIKE '%摩根%' OR broker_name LIKE '%高盛%'`\n"
            "6. 若需確認欄位名稱與最新日期，可調用 `get_stock_schema`。\n"
            "7. 若需即時外部新聞，可調用 `web_search`。\n\n"
            f"{stock_summary}"
        )

        has_system = False
        for m in messages:
            if m.get("role") == "system":
                m["content"] = str(m.get("content", "")) + "\n\n" + system_instruction
                has_system = True
                break
        if not has_system:
            messages.insert(0, {"role": "system", "content": system_instruction})

        # 強制只使用 Gateway 本地工具清單（屏蔽客戶端 tools，防止模型嘗試執行本地 Windows shell/檔案指令）
        payload["tools"] = list(GATEWAY_TOOLS)
        if "temperature" not in payload:
            payload["temperature"] = 0.2

        # Force disable thinking chain in Qwen chat template
        if "chat_template_kwargs" not in payload or not isinstance(payload.get("chat_template_kwargs"), dict):
            payload["chat_template_kwargs"] = {}
        payload["chat_template_kwargs"]["enable_thinking"] = False

        # Guard against small max_tokens from clients (e.g. Cline default 4096)
        if "max_tokens" in payload and isinstance(payload["max_tokens"], int) and payload["max_tokens"] < 8192:
            payload["max_tokens"] = 16384

        MAX_LOOPS = 5
        executed_tool_count = 0

        for loop_idx in range(MAX_LOOPS):
            upstream_resp = self._call_upstream_non_stream(payload)
            if not upstream_resp:
                self._send_json_response(502, {"error": {"message": "模型後端無響應或通訊錯誤", "type": "gateway_upstream_error"}})
                return

            choices = upstream_resp.get("choices", [])
            if not choices:
                self._send_json_response(200, upstream_resp)
                return

            msg = choices[0].get("message", {})
            tool_calls = msg.get("tool_calls")

            # Fallback: Parse raw Qwen <tool_call> tags from content if upstream did not structure them
            content = msg.get("content") or ""
            if not tool_calls and "<tool_call>" in content:
                raw_calls = parse_raw_qwen_tool_calls(content)
                if raw_calls:
                    print(f"[Gateway] 從文字內容中成功解析出 {len(raw_calls)} 個原生 Qwen Tool Call", flush=True)
                    tool_calls = raw_calls
                    msg["tool_calls"] = tool_calls
                    msg["content"] = None

            # Case A: Model wants to execute tool calls
            if tool_calls:
                executed_tool_count += len(tool_calls)
                messages.append(msg)

                for tc in tool_calls:
                    fn_name = tc.get("function", {}).get("name")
                    fn_args_str = tc.get("function", {}).get("arguments", "{}")
                    call_id = tc.get("id", f"call_{int(time.time())}")

                    try:
                        fn_args = json.loads(fn_args_str) if isinstance(fn_args_str, str) else fn_args_str
                    except Exception:
                        fn_args = {}

                    print(f"[Gateway] 執行 Tool: {fn_name}({fn_args})", flush=True)

                    if fn_name == "query_stock_data":
                        sql = fn_args.get("sql", "")
                        sql = clean_sql_string(sql)
                        tool_output = self.server.stock_engine.execute_query(sql)
                    elif fn_name == "get_stock_schema":
                        tbl = fn_args.get("table_name", "")
                        tool_output = self.server.stock_engine.get_schema(tbl)
                    elif fn_name == "web_search":
                        q = fn_args.get("query", "")
                        results = search_searxng(q, self.server.searxng_url)
                        tool_output = json.dumps(results, ensure_ascii=False)
                    else:
                        tool_output = f"[未知的工具名稱: {fn_name}]"

                    messages.append({
                        "role": "tool",
                        "tool_call_id": call_id,
                        "name": fn_name,
                        "content": str(tool_output)
                    })

                # Continue next iteration of loop to let model reason on the tool results
                continue

            # Case B: Model completed reasoning, ready to return final answer
            if executed_tool_count > 0:
                print(f"[Gateway] 工具調用完成 (共執行 {executed_tool_count} 次)，生成最終分析回覆", flush=True)
                if client_wants_stream:
                    # 使用已生成的 msg 合成 SSE 回傳給客戶端，絕對不外洩 tool_calls 給前端
                    self._send_synthesized_sse(msg, upstream_resp.get("model", ""), upstream_resp.get("id", ""))
                else:
                    self._send_json_response(200, upstream_resp)
                return
            else:
                # No tools were invoked
                if client_wants_stream:
                    self._send_synthesized_sse(msg, upstream_resp.get("model", ""), upstream_resp.get("id", ""))
                else:
                    self._send_json_response(200, upstream_resp)
                return

        # Fallback if max loops reached: 移除 tools 強制模型生成最後的文字總結
        print(f"[Gateway] 達到最大工具調用循環上限 ({MAX_LOOPS})，強制模型總結輸出", flush=True)
        payload_final = dict(payload)
        payload_final.pop("tools", None)
        payload_final.pop("tool_choice", None)
        messages.append({
            "role": "user",
            "content": "請根據前面查詢到的所有量化數據，直接產出完整的量化分析與總結回覆，無需再調用任何工具。"
        })

        final_resp = self._call_upstream_non_stream(payload_final)
        if final_resp and final_resp.get("choices"):
            final_msg = final_resp["choices"][0].get("message", {})
            if client_wants_stream:
                self._send_synthesized_sse(final_msg, final_resp.get("model", ""), final_resp.get("id", ""))
            else:
                self._send_json_response(200, final_resp)
        else:
            fallback_msg = {"role": "assistant", "content": "【分析完成】已完成本機量化資料庫統計與分析，詳細數據如上。"}
            if client_wants_stream:
                self._send_synthesized_sse(fallback_msg, "huihui-qwen3.8-27b-abliterated", f"chatcmpl-{int(time.time()*1000)}")
            else:
                self._send_json_response(200, {"choices": [{"message": fallback_msg, "finish_reason": "stop"}]})



def main():
    parser = argparse.ArgumentParser(description="TensorFold DuckDB Stock & Web Gateway")
    parser.add_argument("--port", type=int, default=1234, help="Port to listen for clients (default 1234)")
    parser.add_argument("--upstream", default="127.0.0.1:1235", help="Upstream TensorFold host:port (default 127.0.0.1:1235)")
    parser.add_argument("--searxng", default="http://127.0.0.1:8080", help="SearXNG API base URL (default http://127.0.0.1:8080)")
    parser.add_argument("--stock-dir", default="/home/blue/stock_data", help="Directory containing stock parquet files (default /home/blue/stock_data)")
    args = parser.parse_args()

    host, port_str = args.upstream.split(":")
    server = ThreadingHTTPServer(("0.0.0.0", args.port), GatewayHandler)
    server.upstream_netloc = (host, int(port_str))
    server.searxng_url = args.searxng

    # Initialize DuckDB Stock Engine
    print("[Gateway] 正在初始化 DuckDB 股票量化分析引擎...")
    server.stock_engine = StockEngine(data_dir=args.stock_dir)

    print(f"============================================================")
    print(f" [TensorFold Smart Gateway v2.0] 啟動成功！")
    print(f" • 監聽埠號 (Client Port) : http://0.0.0.0:{args.port}/v1")
    print(f" • 模型核心 (TensorFold)  : http://{args.upstream}/v1")
    print(f" • 搜尋引擎 (SearXNG)     : {args.searxng}")
    print(f" • 股票數據 (DuckDB)      : {args.stock_dir}")
    print(f" • 支援工具 (Tool Calling): query_stock_data, get_stock_schema, web_search")
    print(f"============================================================", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nGateway 正在關閉...", flush=True)
        server.server_close()

if __name__ == "__main__":
    main()
