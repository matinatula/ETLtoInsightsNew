# src/analytics/visualizations.py

from src.etl.db import get_engine
import os
import glob
import pandas as pd
import logging
import plotly.express as px
from jinja2 import Template

# Fix import path for Docker
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

engine = get_engine()

# =============================
# Paths
# =============================
BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))
KPI_FOLDER = os.path.join(BASE_DIR, "sql", "analytics_queries")

REPORT_FOLDER = os.path.join(BASE_DIR, "reports")
CSV_FOLDER = os.path.join(REPORT_FOLDER, "csv")
HTML_FOLDER = os.path.join(REPORT_FOLDER, "interactive")

for folder in [CSV_FOLDER, HTML_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# =============================
# Utility: Run SQL File
# =============================


def run_sql_file(path):
    with open(path, "r") as f:
        query = f.read()
    return pd.read_sql(query, engine)


# =============================
# Run All KPIs
# =============================
def run_all_kpis():
    sql_files = glob.glob(os.path.join(KPI_FOLDER, "*.sql"))
    results = {}

    for file_path in sql_files:
        name = os.path.basename(file_path)
        logging.info(f"Running KPI: {name}")

        try:
            df = run_sql_file(file_path)
            results[name] = df

            # Save CSV with "|" separator
            csv_name = name.replace(".sql", ".csv")
            df.to_csv(os.path.join(CSV_FOLDER, csv_name), index=False, sep="|")

        except Exception as e:
            logging.error(f"Failed KPI {name}: {e}")

    return results


# =============================
# Visualization Logic
# =============================
def visualize_kpis(results):
    interactive_files = []

    for name, df in results.items():
        if df.empty:
            continue

        df = df.fillna("Unknown")
        first_col = df.columns[0].lower()
        second_col = df.columns[1] if len(df.columns) > 1 else None
        title = name.replace(".sql", "").replace("_", " ").title()

        fig = None
        # Time-based chart
        if "date" in first_col or "month" in first_col:
            fig = px.line(
                df, x=df.columns[0], y=df.columns[1], title=title, markers=True)
            fig.update_xaxes(rangeslider_visible=True)
            fig.update_layout(height=500, width=900)

        # Categorical chart (employee/department)
        elif "employee" in first_col or "department" in first_col:
            top_n = min(10, len(df))
            df_top = df.sort_values(df.columns[1], ascending=False).head(top_n)
            fig = px.bar(
                df_top, x=df_top.columns[0], y=df_top.columns[1],
                title=title, hover_data=df_top.columns
            )
            fig.update_layout(height=500, width=900)

        else:
            # Skip single-value KPIs
            continue

        html_file = os.path.join(HTML_FOLDER, name.replace(".sql", ".html"))
        fig.write_html(html_file)
        interactive_files.append((title, html_file))
        logging.info(
            f"Saved interactive HTML: {name.replace('.sql', '.html')}")

    return interactive_files


# =============================
# Build HTML Dashboard
# =============================
def build_dashboard(interactive_files):
    template = """
    <html>
    <head>
        <title>KPI Dashboard</title>
        <style>
            body { font-family: Arial; margin: 20px; }
            h2 { margin-top: 40px; }
            iframe { border: 1px solid #ccc; margin-bottom: 40px; }
        </style>
    </head>
    <body>
        <h1>KPI Dashboard</h1>
        {% for title, html_file in charts %}
            <h2>{{ title }}</h2>
            <iframe src="{{ html_file }}" width="100%" height="500px"></iframe>
        {% endfor %}
    </body>
    </html>
    """
    t = Template(template)
    html_content = t.render(
        charts=[(t, os.path.relpath(f, REPORT_FOLDER))
                for t, f in interactive_files]
    )

    dashboard_file = os.path.join(REPORT_FOLDER, "index.html")
    with open(dashboard_file, "w") as f:
        f.write(html_content)

    logging.info(f"Dashboard created: {dashboard_file}")


# =============================
# MAIN
# =============================
if __name__ == "__main__":
    logging.info("Starting KPI Analytics & Visualization")

    kpi_results = run_all_kpis()

    # Optional: preview top 5 rows of all KPIs
    for kpi, df in kpi_results.items():
        print(f"\n===== {kpi} =====")
        print(df.head(5))

    interactive_files = visualize_kpis(kpi_results)
    build_dashboard(interactive_files)

    logging.info("All KPIs processed successfully.")
