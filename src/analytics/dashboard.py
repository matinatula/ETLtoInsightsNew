import os
import pandas as pd
from src.etl.db import get_engine

KPI_FOLDER = "sql/analytics_queries"


def load_sql(file_path):
    with open(file_path, "r") as f:
        return f.read()


def generate_kpi_summary():
    engine = get_engine()
    summary_rows = []

    for file in sorted(os.listdir(KPI_FOLDER)):
        if file.endswith(".sql"):
            print(f"Running {file}")
            sql = load_sql(os.path.join(KPI_FOLDER, file))
            df = pd.read_sql(sql, engine)

            if not df.empty:
                summary_rows.append({
                    "kpi_name": file.replace(".sql", ""),
                    "rows_returned": len(df),
                    "top_value": df.iloc[0].to_dict()
                })
            else:
                summary_rows.append({
                    "kpi_name": file.replace(".sql", ""),
                    "rows_returned": 0,
                    "top_value": None
                })

    summary_df = pd.DataFrame(summary_rows)

    # Save summary
    summary_df.to_csv("kpi_summary_dashboard.csv", index=False)

    print("\n KPI Summary Dashboard saved as kpi_summary_dashboard.csv")


if __name__ == "__main__":
    generate_kpi_summary()
