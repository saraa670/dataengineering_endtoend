import os
import prefect

prefect.settings.PREFECT_API_URL = "http://127.0.0.1:4200/api"

flows = [
    "sales_pipeline.py",
    "hr_pipeline.py",
    "customer_pipeline.py",
    "production_pipeline.py"
]

print("\n====== RUNNING ALL PIPELINES ======\n")

for f in flows:
    print(f"\n--- Running {f} ---")
    os.system(f"python {f}")

print("\n====== ALL PIPELINES COMPLETED ======\n")
