import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from src.etl.db import get_engine

KPI_FOLDER = "sql/analytics_queries"


def load_sql(path):
    with open(path, "r") as f:
        return f.read()


def run_kpi(engine, file):
    sql = load_sql(os.path.join(KPI_FOLDER, file))
    return pd.read_sql(sql, engine)


def generate_dashboard():
    engine = get_engine()

    # --- RUN KEY KPIs FOR SUMMARY CARDS ---
    headcount_df = run_kpi(engine, "current_headcount_by_department.sql")
    attrition_df = run_kpi(engine, "early_attrition_rate.sql")
    overtime_df = run_kpi(engine, "total_overtime_count.sql")
    irregular_df = run_kpi(engine, "attendance_irregularity_score.sql")

    total_headcount = headcount_df["active_headcount"].sum()
    attrition_rate = attrition_df.iloc[0, 0]
    total_overtime_days = overtime_df["overtime_days"].sum()
    top_irregular_employee = irregular_df.iloc[0]

    # --- BUILD FIGURE ---
    fig = make_subplots(
        rows=6,
        cols=1,
        subplot_titles=[
            "Active Headcount Over Time",
            "Monthly Hiring Trend",
            "Turnover Trend",
            "Late Arrival Frequency",
            "Average Working Hours per Employee",
            "Overtime by Department"
        ],
        vertical_spacing=0.08
    )

    # 1️⃣ Active Headcount
    df = run_kpi(engine, "active_headcount_over_time.sql")
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["active_headcount"], mode="lines"), row=1, col=1)

    # 2️⃣ Hiring Trend
    df = run_kpi(engine, "monthly_hiring_trend.sql")
    fig.add_trace(go.Scatter(
        x=df["month"], y=df["hires"], mode="lines"), row=2, col=1)

    # 3️⃣ Turnover
    df = run_kpi(engine, "turnover_trend.sql")
    fig.add_trace(go.Bar(x=df["month"], y=df["terminations"]), row=3, col=1)

    # 4️⃣ Late Arrivals
    df = run_kpi(engine, "late_arrival_frequency.sql").head(10)
    fig.add_trace(go.Bar(x=df["client_employee_id"].astype(str),
                         y=df["late_arrivals"]), row=4, col=1)

    # 5️⃣ Avg Working Hours
    df = run_kpi(engine, "average_working_hours_per_employee.sql").head(10)
    fig.add_trace(go.Bar(x=df["client_employee_id"].astype(str),
                         y=df["avg_daily_hours"]), row=5, col=1)

    # 6️⃣ Overtime by Dept
    df = run_kpi(engine, "avg_overtime_by_department.sql").head(10)
    fig.add_trace(go.Bar(x=df["department_id"].astype(str),
                         y=df["avg_hours_worked"]), row=6, col=1)

    fig.update_layout(
        height=3200,
        width=1200,
        title={
            "text": f"<b>Workforce Analytics Executive Dashboard</b><br>"
            f"Generated on {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            f"<br><br>"
            f"<b>Total Headcount:</b> {total_headcount} | "
            f"<b>Early Attrition Rate:</b> {attrition_rate:.2%} | "
            f"<b>Total Overtime Days:</b> {total_overtime_days} | "
            f"<b>Top Irregular Employee:</b> {top_irregular_employee['client_employee_id']} "
            f"({int(top_irregular_employee['attendance_irregularities'])})"
        },
        title_x=0.5,
        showlegend=False
    )

    fig.write_html("kpi_dashboard.html")
    print("✅ Structured Executive Dashboard saved as kpi_dashboard.html")


if __name__ == "__main__":
    generate_dashboard()
