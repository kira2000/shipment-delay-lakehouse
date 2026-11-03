# Databricks notebook source
# MAGIC %md
# MAGIC # 07 Report agent: core
# MAGIC Definitions only (no demo), so other notebooks can `%run ./07_agent_core`.
# MAGIC
# MAGIC **Flow:** question -> LLM writes SQL from the live schema -> guardrail validator -> run -> (one repair retry on error) -> LLM writes a short answer from the result rows only -> faithfulness check (every number in the answer must exist in the result).
# MAGIC
# MAGIC **Why these guardrails:** the model never sees raw data it was not asked about, can only read the allow-listed gold tables, can only run one SELECT, and every answer ships with the SQL that produced it so a human can audit it.
# MAGIC
# MAGIC **Model:** a Databricks-hosted open model through `ai_query` (Free Edition blocks the premium Claude/GPT endpoints). Set `MODEL` to an endpoint name listed under *Serving* in your workspace.

# COMMAND ----------

import re, json, decimal, datetime

CATALOG = "workspace"
SCHEMA = "olist"
MODEL = "databricks-meta-llama-3-3-70b-instruct"   # change if your Serving tab shows a different name

ALLOWED_TABLES = [
    "dim_customer", "dim_seller", "dim_product",
    "fact_order_items", "fact_orders",
    "kpi_seller_delivery", "kpi_state_delivery", "kpi_category_delivery",
    "kpi_monthly", "kpi_late_vs_review", "kpi_eta_gap_state",
]
MAX_ROWS = 200

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LLM call

# COMMAND ----------

def llm(prompt: str, max_tokens: int = 600) -> str:
    """One chat completion through ai_query, temperature 0 for repeatable evals."""
    row = spark.sql(
        "SELECT ai_query(:m, :p, modelParameters => named_struct('temperature', 0.0, 'max_tokens', :n)) AS r",
        args={"m": MODEL, "p": prompt, "n": max_tokens},
    ).collect()[0]
    return row["r"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema context (read live from Unity Catalog, so it never drifts from the tables)

# COMMAND ----------

BUSINESS_RULES = """
Business rules (follow exactly):
- Dataset: Olist Brazilian e-commerce orders, 2016-2018. All money is in BRL.
- "Late" means delivered to the customer AFTER the estimated delivery date. Column is_late (boolean) on fact_orders / fact_order_items.
- Delivery KPIs are only valid for delivered orders: use is_delivered = true AND is_late IS NOT NULL.
- late_rate in the kpi_* tables is already a fraction between 0 and 1 (0.08 = 8%). Report it as a percentage in words.
- kpi_seller_delivery / kpi_state_delivery / kpi_category_delivery / kpi_monthly are pre-aggregated: prefer them over the fact tables when they answer the question.
- To rank sellers/states/categories by late_rate, ignore tiny groups: require delivered_orders >= 100 for states and categories, >= 50 for sellers.
- kpi_monthly.month is the first day of the month (a timestamp).
- Return RAW numeric values. Never multiply by 100, never use ROUND, CONCAT, FORMAT or any text formatting on numbers; rates stay fractions (0.0812). Percent conversion happens later in the written answer.
- Never give a SELECT alias the same name as an existing column (it breaks ORDER BY). Keep original column names.
- Overall / total / all-time figures: aggregate across ALL rows (e.g. SUM(late_orders) / SUM(delivered_orders) from kpi_monthly, or an aggregate over fact_orders). Never use LIMIT 1 on an unaggregated table to answer an overall question.
- Count ORDERS with COUNT(DISTINCT order_id). fact_order_items has one row per item, so never count its rows as orders. For order-level averages (delivery days, review score, order value) use fact_orders.
- "Top N by X, then pick by Y": rank in a subquery or CTE ordered by X with LIMIT N, then order that result by Y in the outer query.
- Differences are reported as (first group - second group) in the order the question names them, as a positive number when the question says "lower by" / "higher by".
- Do not invent columns. Use only columns shown in the schema below.
""".strip()

def build_schema_context() -> str:
    lines = []
    for tname in ALLOWED_TABLES:
        cols = spark.sql(f"DESCRIBE TABLE {CATALOG}.{SCHEMA}.{tname}").collect()
        coldefs = ", ".join(f"{c['col_name']} {c['data_type']}" for c in cols
                            if c["col_name"] and not c["col_name"].startswith("#"))
        lines.append(f"- {tname}({coldefs})")
    return "\n".join(lines)

SCHEMA_CONTEXT = build_schema_context()
print(SCHEMA_CONTEXT[:1500], "...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guardrails: read-only SQL validator

# COMMAND ----------

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|replace|grant|revoke|"
    r"copy|optimize|vacuum|call|exec|execute|set|use|refresh|msck|export|load|put)\b", re.I)

def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return re.sub(r"--[^\n]*", " ", sql)

class GuardrailError(Exception):
    pass

def validate_sql(sql: str) -> str:
    """Return a safe SQL string (with a LIMIT) or raise GuardrailError."""
    s = _strip_comments(sql).strip().rstrip(";").strip()
    # function forms like EXTRACT(YEAR FROM col) / TRIM(BOTH x FROM col) are not table references
    s_refs = re.sub(r"\b(extract|trim|substring|overlay)\s*\([^()]*?\bfrom\b[^()]*?\)", " fn() ", s, flags=re.I)
    if not s:
        raise GuardrailError("empty SQL")
    if ";" in s:
        raise GuardrailError("multiple statements are not allowed")
    if not re.match(r"^(select|with)\b", s, re.I):
        raise GuardrailError("only SELECT / WITH queries are allowed")
    bad = FORBIDDEN.search(s)
    if bad:
        raise GuardrailError(f"forbidden keyword: {bad.group(1).upper()}")
    # every table after FROM / JOIN must be in the allow-list (CTE names are exempt)
    ctes = set(re.findall(r"\b([a-zA-Z_][\w]*)\s+as\s*\(", s_refs, re.I))
    refs = re.findall(r"\b(?:from|join)\s+([`\w\.]+)", s_refs, re.I)
    for r in refs:
        name = r.replace("`", "").split(".")[-1].lower()
        if name in ctes:
            continue
        if name not in ALLOWED_TABLES:
            raise GuardrailError(f"table not allowed: {name}")
    if not re.search(r"\blimit\s+\d+\s*$", s, re.I):
        s = f"SELECT * FROM (\n{s}\n) LIMIT {MAX_ROWS}"
    return s

# quick self-test of the validator
for bad_sql in ["DROP TABLE fact_orders", "SELECT * FROM silver_orders",
                "SELECT 1; DELETE FROM fact_orders", "INSERT INTO fact_orders VALUES (1)",
                "WITH x AS (SELECT 1) SELECT * FROM x JOIN raw_secrets s ON 1=1"]:
    try:
        validate_sql(bad_sql); print("NOT BLOCKED:", bad_sql)
    except GuardrailError as e:
        print("blocked ->", e)
print("ok ->", validate_sql("SELECT customer_state, late_rate FROM kpi_state_delivery ORDER BY late_rate DESC")[:60])

# COMMAND ----------

# MAGIC %md
# MAGIC ## The agent

# COMMAND ----------

SQL_PROMPT = """You are a careful analytics engineer writing Spark SQL (Databricks) for a business user.
Return ONLY one SQL query inside a ```sql code block. No explanation.

{rules}

Tables (name(column type, ...)):
{schema}

Question: {question}
{extra}"""

ANSWER_PROMPT = """You are writing a short answer for a business user.
Use ONLY the numbers in the result rows below. Do not compute new numbers, do not use outside knowledge.
Write 1-3 plain sentences. Convert fractions like 0.0812 to percentages like 8.1%. Round sensibly.
If the result is empty, say that no rows matched.

Question: {question}
Result rows (JSON): {rows}"""

def extract_sql(text: str) -> str:
    m = re.search(r"```(?:sql)?\s*(.*?)```", text, re.S | re.I)
    return (m.group(1) if m else text).strip()

def _jsonable(v):
    if isinstance(v, decimal.Decimal): return float(v)
    if isinstance(v, (datetime.date, datetime.datetime)): return v.isoformat()
    return v

def run_sql(safe_sql: str):
    df = spark.sql(safe_sql)
    cols = df.columns
    rows = [{c: _jsonable(r[c]) for c in cols} for r in df.collect()]
    return cols, rows

def _numbers_in(text: str):
    vals = []
    for m in re.findall(r"-?\d[\d,]*\.?\d*", text):
        try: vals.append(float(m.replace(",", "")))
        except ValueError: pass
    return vals

def faithfulness(answer: str, rows, question: str = "") -> dict:
    """Every number quoted in the answer must be traceable to a result value
    (as-is, x100 for percentages, or rounded). Returns unsupported numbers."""
    pool = []
    for r in rows:
        for v in r.values():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                pool += [v, v * 100]
    asked = set(_numbers_in(question))          # numbers the user typed are not invented
    unsupported = []
    for n in _numbers_in(answer):
        if n in asked:
            continue
        ok = any(abs(n - p) <= max(0.06, 0.006 * abs(p)) for p in pool)   # ~rounding tolerance
        if not ok:
            unsupported.append(n)
    return {"faithful": len(unsupported) == 0, "unsupported_numbers": unsupported}

def ask(question: str, answer: bool = True) -> dict:
    out = {"question": question, "sql": None, "columns": None, "rows": None,
           "answer": None, "status": "ok", "error": None, "repaired": False,
           "faithful": None, "unsupported_numbers": None}
    prompt = SQL_PROMPT.format(rules=BUSINESS_RULES, schema=SCHEMA_CONTEXT, question=question, extra="")
    try:
        raw_sql = extract_sql(llm(prompt))
        out["sql"] = raw_sql
        try:
            safe = validate_sql(raw_sql)
        except GuardrailError as e:
            out.update(status="blocked", error=str(e)); return out
        try:
            cols, rows = run_sql(safe)
        except Exception as e:                      # one repair attempt with the error message
            err = str(e)[:600]
            retry = SQL_PROMPT.format(rules=BUSINESS_RULES, schema=SCHEMA_CONTEXT, question=question,
                extra=f"\nYour previous SQL failed.\nSQL: {raw_sql}\nError: {err}\nReturn a corrected query.")
            raw_sql = extract_sql(llm(retry)); out["sql"] = raw_sql; out["repaired"] = True
            try:
                safe = validate_sql(raw_sql)
            except GuardrailError as e2:
                out.update(status="blocked", error=str(e2)); return out
            cols, rows = run_sql(safe)
        out["columns"], out["rows"] = cols, rows
        if answer:
            out["answer"] = llm(ANSWER_PROMPT.format(question=question, rows=json.dumps(rows[:50])), 250).strip()
            out.update(faithfulness(out["answer"], rows, question))
    except Exception as e:
        out.update(status="error", error=str(e)[:500])
    return out

def show(res: dict):
    print("Q:", res["question"]); print("STATUS:", res["status"], "| repaired:", res["repaired"])
    print("SQL:\n", res["sql"])
    if res["error"]: print("ERROR:", res["error"])
    if res["rows"] is not None: print("ROWS (first 5):", res["rows"][:5])
    if res["answer"]: print("ANSWER:", res["answer"], "| faithful:", res["faithful"], res["unsupported_numbers"])
