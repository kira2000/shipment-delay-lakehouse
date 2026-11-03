# Databricks notebook source
# MAGIC %md
# MAGIC # 09 Weekly delivery brief
# MAGIC Produces the weekly executive brief as markdown. The numbers come from **fixed, reviewed SQL** (not model-written SQL), so the scheduled report is deterministic. The LLM only writes the narrative from those numbers, and the faithfulness check flags any figure it invents. Each section lists the SQL beside its numbers.
# MAGIC
# MAGIC **Schedule it:** Jobs & Pipelines -> Create job -> notebook task -> this notebook -> Schedule: weekly (Monday 08:00).

# COMMAND ----------

# MAGIC %run ./07_agent_core

# COMMAND ----------

import datetime as dt

SECTIONS = [
 ("Headline", """
  SELECT SUM(delivered_orders) AS delivered_orders, SUM(late_orders) AS late_orders,
         SUM(late_orders) * 1.0 / SUM(delivered_orders) AS late_rate,
         SUM(avg_delivery_days * delivered_orders) / SUM(delivered_orders) AS avg_delivery_days
  FROM kpi_monthly"""),
 ("Latest 6 months (late rate trend)", """
  SELECT date_format(month, 'yyyy-MM') AS month, delivered_orders, late_rate
  FROM kpi_monthly WHERE delivered_orders >= 100 ORDER BY month DESC LIMIT 6"""),
 ("Worst 5 states by late rate (100+ orders)", """
  SELECT customer_state, delivered_orders, late_rate, avg_delivery_days
  FROM kpi_state_delivery WHERE delivered_orders >= 100 ORDER BY late_rate DESC LIMIT 5"""),
 ("Worst 5 categories by late rate (100+ orders)", """
  SELECT category, delivered_orders, late_rate
  FROM kpi_category_delivery WHERE delivered_orders >= 100 ORDER BY late_rate DESC LIMIT 5"""),
 ("Sellers to review (50+ orders, late rate above 20%)", """
  SELECT seller_id, seller_state, delivered_orders, late_orders, late_rate
  FROM kpi_seller_delivery WHERE delivered_orders >= 50 AND late_rate > 0.2
  ORDER BY late_orders DESC LIMIT 10"""),
 ("Customer impact", """
  SELECT is_late, orders, avg_review_score, share_1_star
  FROM kpi_late_vs_review ORDER BY is_late"""),
]

NARRATIVE_PROMPT = """You are writing the opening summary of a weekly delivery performance brief for an operations leader.
Use ONLY the numbers in the data below. Do not compute new numbers. Convert fractions to percentages (0.081 -> 8.1%).
Write 4-6 short bullet points: what the headline is, what changed recently, where the problem concentrates (states, categories, sellers), and the customer impact. End with one recommended action.

DATA (JSON by section):
{data}"""

results = {}
for title, sql in SECTIONS:
    cols, rows = run_sql(validate_sql(sql))
    results[title] = dict(sql=" ".join(sql.split()), rows=rows)

all_rows = [r for s in results.values() for r in s["rows"]]
narr = llm(NARRATIVE_PROMPT.format(data=json.dumps({k: v["rows"] for k, v in results.items()})), 500).strip()
def _strip_non_figures(text):
    """Dates (2018-03), long IDs (seller hashes) and '1-star' are labels, not reported figures."""
    text = re.sub(r"\b\d{4}-\d{2}(-\d{2})?\b", " ", text)
    text = re.sub(r"\b[0-9a-f]{16,}\b", " ", text)
    return re.sub(r"\b\d+-star\b", " ", text, flags=re.I)

check = faithfulness(_strip_non_figures(narr), all_rows)

def md_table(rows):
    if not rows: return "_no rows_"
    cols = list(rows[0].keys())
    def f(v):
        return f"{v:.3f}" if isinstance(v, float) else str(v)
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)] +
                     ["| " + " | ".join(f(r[c]) for c in cols) + " |" for r in rows])

today = dt.date.today().isoformat()
brief = [f"# Weekly delivery brief ({today})", "", "## Summary", narr, "",
         f"_Faithfulness check: {'passed' if check['faithful'] else 'REVIEW - unsupported numbers ' + str(check['unsupported_numbers'])}_", ""]
for title, v in results.items():
    brief += [f"## {title}", md_table(v["rows"]), "", "<details><summary>SQL</summary>", "", "```sql", v["sql"], "```", "</details>", ""]
brief_md = "\n".join(brief)

# COMMAND ----------

out_dir = f"/Volumes/{CATALOG}/{SCHEMA}/raw/briefs"
dbutils.fs.mkdirs(out_dir)
path = f"{out_dir}/weekly_brief_{today}.md"
dbutils.fs.put(path, brief_md, True)
print("saved", path)
print(brief_md)
