"""
Create a Databricks service principal for the retail capstone project.

Phase 5: Production Governance and MLOps

Purpose:
- Demonstrates how a production job identity can be created with the Databricks SDK.
- The service principal can later be granted permissions to run Databricks Jobs,
  access Unity Catalog tables, and write pipeline outputs.

Important:
- This script requires Databricks workspace admin permissions.
- Do not commit tokens or secrets to GitHub.
- Authenticate using environment variables or Databricks CLI authentication.
"""

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam


SERVICE_PRINCIPAL_NAME = "retail-capstone-job-sp"


def main() -> None:
    """Create a Databricks service principal if permissions allow."""

    workspace_client = WorkspaceClient()

    print(f"Creating service principal: {SERVICE_PRINCIPAL_NAME}")

    service_principal = workspace_client.service_principals.create(
        display_name=SERVICE_PRINCIPAL_NAME,
        active=True
    )

    print("Service principal created successfully.")
    print(f"Display name: {service_principal.display_name}")
    print(f"Application ID: {service_principal.application_id}")
    print(f"ID: {service_principal.id}")


if __name__ == "__main__":
    main()
