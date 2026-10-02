# TensorFold Gateway 升級規劃文件：DuckDB + Tool Calling 智慧股票分析網關

> **版本**：v2.0 規劃草案  
> **日期**：2026-10-01  
> **目標**：將現有簡易關鍵字/URL 檢索 Gateway 改造為具備 **DuckDB 本地高維數據庫查詢** 與 **原生 OpenAI 規範 Tool Calling** 的智慧金融分析代理。當使用者詢問股票相關問題時，自動執行 SQL 分析 `/home/blue/stock_data/` 的 Parquet 資料並生成專業量化分析回覆。

---

## 1. 目前 Gateway 功能檢視與問題分析

### 1.1 現有架構工作流程
現有的 `gateway/gateway.py` 是一個基於 Python 原生 `http.server.BaseHTTPRequestHandler` 的輕量反向代理，監聽 `0.0.0.0:1234` 並轉發至 TensorFold 後端 `127.0.0.1:1235`：

1. **粗粒度關鍵字匹配**：使用 `SEARCH_TRIGGERS` 列表比對使用者字串（如「搜尋」、「最新」、「今天」）或正則表達式 `URL_REGEX` 檢測網址。
2. **提示詞硬編碼注入（Prompt Injection）**：
   - 若符合搜尋意圖，在代理端向本地 SearXNG (`http://127.0.0.1:8080`) 發出 HTTP 查詢。
   - 將文字拼接在使用者 Prompt 後方（`content + search_info + "\n請綜合以上網路參考資料..."`）。
3. **直通轉發**：將竄改後的請求發送給 TensorFold 模型，並將回傳串流直接寫回客戶端。

### 1.2 現有架構的局限性與痛點
1. **無結構化數據分析能力**：無法讀取本地海量量化資料庫，無法解答「台積電過去一個月外資籌碼與融資變化」、「今天成交量前十名且漲幅大於 3% 的股票」、「三大法人期貨淨部位走勢」等專業問題。
2. **依賴剛性關鍵字，容易誤判或漏判**：非關鍵字的問題不會觸發檢索；只要有提及「今天」等字眼就會無差別發送 SearXNG 查詢。
3. **缺乏模型自主決策（Tool Calling）**：模型無法根據對話上下文主動決定「要查哪一張資料表」、「何時需要查詢」、「缺少哪些參數需要進一步檢索」。
4. **上下文浪費與污染**：硬注入整個網頁搜尋結果，容易耗損 Context Window，且無法做到多輪工具調用反饋（Agent Loop）。

---

## 2. 升級目標與新架構設計

### 2.1 核心設計原則
- **原生 Tool Calling**：遵循 OpenAI 標準 Function Calling 規範（`tools`、`tool_calls`、`role: "tool"`），讓 Qwen 3.8 Flash Next 自主決定是否呼叫工具。
- **DuckDB 嵌入式極速引擎**：直接對 `/home/blue/stock_data/*.parquet` 進行向量化 SQL 查詢，免 ETL 匯入、零拷貝（Zero-copy）、毫秒級回傳統計數據。
- **Gateway 自治式代理循環（Autonomous Server-side Agent Loop）**：
  - 客戶端（Cline / VS Code / LM Studio / 聊天介面）發送標準對話請求。
  - Gateway 在轉發至 TensorFold 時自動注入工具定義清單（DuckDB 股票查詢、SearXNG 聯網搜尋）。
  - 若模型決定觸發 `tool_calls`，Gateway 在後端攔截並在本地執行 DuckDB SQL / SearXNG 查詢，再將工具執行結果組裝回上下文，遞迴調用 TensorFold，直到模型給出最終回答。
  - **客戶端完全無感**，無論客戶端是否原生支援工具調用，都能直接獲得精準的數據分析結果，並完美支援 SSE Streaming 輸出。
- **唯讀與安全限制**：DuckDB 連線採用唯讀或臨時記憶體模式，限制查詢時間（Timeout 10s）、限制最大筆數（預設最多 100 筆，防止炸裂 Context）。

### 2.2 架構架構圖 (Architecture Flow)

```mermaid
flowchart TD
    Client["客戶端 (LM Studio / Cline / Chat UI)"]
    GW["智慧 Gateway (0.0.0.0:1234)"]
    TF["TensorFold 模型 (127.0.0.1:1235)<br/>Qwen 3.8 Flash Next"]
    DuckDB["DuckDB 嵌入式查詢引擎"]
    StockDir["/home/blue/stock_data/<br/>5 大 Parquet 數據集"]
    SearXNG["SearXNG (8080)<br/>聯網搜尋"]

    Client -->|1. POST /v1/chat/completions| GW
    GW -->|2. 注入 Tools Schema 轉發| TF
    
    TF -- "3a. 不需要工具 (一般閒聊/代碼)" --> GW
    GW -->|3b. 串流回應| Client

    TF -- "4a. 產生 Tool Call (如 SQL 查詢)" --> GW
    GW -->|4b. 執行 SQL| DuckDB
    DuckDB -->|4c. 零拷貝查詢| StockDir
    DuckDB -->|4d. 查詢結果 (Markdown Table / JSON)| GW
    GW -->|4e. 攜帶 tool 角色結果二度請求| TF
    TF -->|5. 根據分析結果生成專業報告| GW
    GW -->|6. 串流返回最終答案| Client
```

---

## 3. `/home/blue/stock_data/` 資料集與 DuckDB 視圖（View）規格

Gateway 啟動時將在 DuckDB 記憶體實例中自動註冊對應的 View，模型只需下達標準 SQL 即可交叉關聯分析：

| 視圖名稱 (View Name) | 對應 Parquet 檔案規則 | 核心欄位說明 | 典型應用場景 |
| :--- | :--- | :--- | :--- |
| **`close_price`** | `api_close1_*.parquet` | `symbol` (股票代碼), `name` (股名), `trade_date` (日期), `market` (上市/上櫃), `open`, `high`, `low`, `close` (收盤), `change` (漲跌), `volume` (成交股數), `turnover` (成交金額) | 股價走勢、均線、強勢股篩選、量價背離、漲跌排行 |
| **`margin`** | `api_margin_*.parquet` | `symbol`, `name`, `trade_date`, `market`, `margin_balance` (融資餘額), `margin_net` (融資淨買賣), `short_balance` (融券餘額), `short_net` (融券淨買賣), `short_margin_ratio_pct` (券資比), `margin_utilization_pct` | 信用交易活絡度、散戶動向、軋空潛力股篩選 |
| **`taifex`** | `api_taifex_*.parquet` | `trade_date`, `foreign_tx_oi` (外資台指期留倉), `investment_tx_oi` (投信留倉), `dealer_tx_oi` (自營商留倉), `foreign_mtx_oi` (外資小台), `retail_mtx_net` (散戶小台淨部位), `retail_mtx_ratio_pct` (散戶多空比), `macro_sentiment` | 宏觀大盤多空、期貨大戶佈局、散戶反指標分析 |
| **`tdcc`** | `api_tdcc_*.parquet` | `symbol`, `trade_date`, `large_shareholder_pct` (千張大戶持股比率), `large_shareholder_count`, `retail_shareholder_pct` (散戶持股比率), `total_shareholders` (總人數) | 籌碼集中度分析、主力吃貨/出貨監控、每週大戶動向 |
| **`revenue`** | `api_revenue_*.parquet` | `symbol`, `name`, `trade_date` (或 `report_month`), `revenue` (當月營收), `mom_pct` (月增率), `yoy_pct` (年增率), `cum_revenue` (累計營收) | 基本面業績成長股篩選、營收雙增/創歷史新高監控 |
| **`broker_trade`** *(擴充)* | `api_absr1_*.parquet` | `symbol`, `trade_date`, `broker_id` (分點代號), `net_vol` (買賣超股數), `net_amt` (買賣超金額), `buy_avg_price`, `sell_avg_price` | 地緣券商、主力分點籌碼進出追蹤 |

> [!NOTE]
> 若某個資料表檔案尚未產生（如 `api_revenue_*.parquet` 目前仍在準備中），Gateway 在初始化 View 時會自動做檔案存在檢查，提供空表結構或降級提示，確保系統不崩潰。

---

## 4. Tool Calling 規格定義 (Schema)

Gateway 將向模型註冊以下核心工具：

### 4.1 工具一：`query_stock_data` (DuckDB SQL 分析工具)
```json
{
  "type": "function",
  "function": {
    "name": "query_stock_data",
    "description": "執行 DuckDB SQL 查詢台股本地量化資料庫 (/home/blue/stock_data/)。支援資料表：close_price (日量價), margin (融資融券), taifex (期貨大戶/散戶部位), tdcc (千張大戶籌碼), revenue (月營收), broker_trade (分點明細)。支援所有標準 DuckDB SQL 語法與分析函數。",
    "parameters": {
      "type": "object",
      "required": ["sql", "reasoning"],
      "properties": {
        "sql": {
          "type": "string",
          "description": "標準 SQL 查詢語句。例如: 'SELECT symbol, name, close, change, volume FROM close_price WHERE trade_date = (SELECT MAX(trade_date) FROM close_price) ORDER BY volume DESC LIMIT 10;'"
        },
        "reasoning": {
          "type": "string",
          "description": "此查詢的目的簡述（例如：取得最新交易日台積電收盤價與近五日籌碼集中度）。"
        }
      }
    }
  }
}
```

### 4.2 工具二：`get_stock_schema` (資料字典查詢工具)
```json
{
  "type": "function",
  "function": {
    "name": "get_stock_schema",
    "description": "取得資料庫中指定資料表的欄位定義、資料型態與近期可用日期區間，防止 SQL 欄位名稱錯誤。",
    "parameters": {
      "type": "object",
      "required": ["table_name"],
      "properties": {
        "table_name": {
          "type": "string",
          "enum": ["close_price", "margin", "taifex", "tdcc", "revenue", "broker_trade"],
          "description": "欲查詢的資料表名稱。"
        }
      }
    }
  }
}
```

### 4.3 工具三：`web_search` (即時新聞聯網搜尋 - 保留原功能)
```json
{
  "type": "function",
  "function": {
    "name": "web_search",
    "description": "當需要查詢台股最新盤勢新聞、公司重訊、國際市場即時動態或非結構化資訊時調用 SearXNG 搜尋。",
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
```

---

## 5. Gateway 代理邏輯實現細節

### 5.1 System Prompt 引導策略
Gateway 會在送交 TensorFold 前，於 messages 的頂部或系統提示詞（System Prompt）追加指引：
> 「你是一名頂尖的台股量化投資分析專家。你擁有本地 DuckDB 量化資料庫查詢工具 `query_stock_data` 與聯網搜尋工具 `web_search`。當使用者詢問股票價格、技術指標、籌碼變化、融資融券、期貨留倉、千張大戶持股或營收基本面時，**請務必調用 `query_stock_data` 執行精確的 SQL 查詢**，嚴禁虛構歷史數據。分析時請結合數據客觀呈現，並提示投資風險。」

### 5.2 多輪工具循環 (Loop Execution)
```python
MAX_TOOL_LOOPS = 4  # 防止死循環

for _ in range(MAX_TOOL_LOOPS):
    # 呼叫 upstream TensorFold (非串流以便擷取 tool_calls)
    response = call_upstream_tensorfold(payload)
    message = response["choices"][0]["message"]
    
    if not message.get("tool_calls"):
        # 模型已完成推論，給出最終文字回答
        return output_to_client(message, stream=original_stream_flag)
    
    # 攔截並執行 Tool Calls
    payload["messages"].append(message)
    for tool_call in message["tool_calls"]:
        func_name = tool_call["function"]["name"]
        args = json.loads(tool_call["function"]["arguments"])
        
        # 執行本地 DuckDB 或 SearXNG
        tool_result = execute_tool(func_name, args)
        
        payload["messages"].append({
            "role": "tool",
            "tool_call_id": tool_call["id"],
            "name": func_name,
            "content": tool_result
        })
```

### 5.3 串流傳輸 (SSE Streaming) 處理
- 當最後一輪推論確定沒有 `tool_calls` 時：
  - 若客戶端要求 `stream: true`，Gateway 直接以 `stream: true` 呼叫 TensorFold，並將 SSE chunks（`data: {...}`）逐字串流寫入客戶端的 HTTP 響應。
  - 若中間在執行 Tool Calling，Gateway 亦可在串流模式下發送自訂的即時狀態提示（如 `data: {"status": "正在分析股票籌碼數據..."}`），大幅提升客戶端等待體驗。

---

## 6. 環境相容性與修改檔案清單

### 6.1 Python 依賴與獨立虛擬環境
由於 Ubuntu 24.04 / Debian 採用 PEP 668 外部管理機制，且本機為 NVIDIA DGX Spark (Linux aarch64 ARM64 架構)：
- 方案：在專案目錄下建立獨立輕量虛擬環境 `gateway/.venv`：
  ```bash
  python3 -m venv gateway/.venv
  gateway/.venv/bin/pip install duckdb
  ```
- 實測確認：PyPI 官方提供相容 `aarch64` 的預編譯 wheel，安裝秒級完成，無需 gcc 編譯。

### 6.2 具體修改檔案清單
1. **`gateway/gateway.py`**（全面升級）：
   - 引入 DuckDB 引擎模組，啟動時掃描 `/home/blue/stock_data/*.parquet` 自動註冊 5 大 View。
   - 移除原有粗糙的 `SEARCH_TRIGGERS` 文字硬編碼拼接，改為原生 Tool Calling 規格宣告。
   - 實作伺服器端 Agent Loop，攔截 `tool_calls` 並執行 DuckDB SQL 與 SearXNG 搜尋。
   - 完善錯誤回退（SQL 語法錯誤時將錯誤訊息回傳給模型重試）。
2. **`gateway/stock_engine.py`**（新增模組）：
   - 專門負責 DuckDB 連線管理、View 建立、SQL 執行安全沙盒（唯讀、超時、限制筆數）。
   - 提供格式化輸出（轉為 Markdown 表格或簡潔 JSON 字串）。
3. **`start_web.sh`**（修改啟動腳本）：
   - 自動檢測 `gateway/.venv` 是否存在，若無自動建立並 `pip install duckdb`。
   - 使用 `gateway/.venv/bin/python3` 啟動 `gateway.py`。
4. **`專案架構與使用說明指南.md`**（同步更新文件）：
   - 更新架構圖，補充 DuckDB 股票資料表字典、SQL 查詢範例與 Tool Calling 模式說明。

---

## 7. 驗證情境規劃

實作完成後，將規劃以下測試情境：

1. **情境 A：大盤與期貨情緒**
   - 測試提問：「幫我分析最新台指期貨三大法人留倉與散戶多空情緒。」
   - 預期：模型呼叫 `query_stock_data` 查詢 `taifex` 視圖，計算 `foreign_tx_oi`、`retail_mtx_ratio_pct` 並給出多空判斷。
2. **情境 B：個股量價與融資變化**
   - 測試提問：「查一下 2330 台積電最近的股價與融資融券變化。」
   - 預期：模型呼叫 `query_stock_data` 關聯 `close_price` 與 `margin` 視圖進行統整分析。
3. **情境 C：千張大戶籌碼集中度**
   - 測試提問：「最近千張大戶持股比例增加最多的是哪些股票？」
   - 預期：模型下達 `tdcc` 視圖的排序與差異 SQL 查詢。
4. **情境 D：一般非股票問題（回退測試）**
   - 測試提問：「請寫一段 Python 快速排序代碼。」
   - 預期：不觸發工具，直接串流快速輸出答案。
5. **情境 E：即時聯網新聞**
   - 測試提問：「搜尋今天最新的科技業大新聞。」
   - 預期：模型調用 `web_search` 工具。

---

## 8. 請使用者確認

本規劃文件涵蓋了現狀分析、DuckDB 視圖設計、OpenAI Tool Calling 協定、Agent 代理調用架構與啟動指令相容性。

**請您審閱以上修改規劃文件，若確認方向與設計符合您的需求，請告知我，我將立即開始進行程式碼實作與驗證！**
