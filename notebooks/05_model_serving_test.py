{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "7c16c0c6-d394-4f24-be4e-2cf76577ee81",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 05 Model Serving Test\n",
    "\n",
    "This notebook tests the registered champion models before or after deployment to Databricks Model Serving.\n",
    "\n",
    "The project includes two registered models:\n",
    "\n",
    "- Stockout Risk Classifier\n",
    "- Reorder Flag Classifier"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "17efa98e-d89d-4672-a23e-e98b27c76a82",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import mlflow\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e652d4c6-0896-468d-9889-eb84b4c1e0cd",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "mlflow.set_registry_uri(\"databricks-uc\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "0d7a41ee-b260-4ae0-8282-25c7ab71f53c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "5fbc27d6351948c6adbf694a65bc920f",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Downloading artifacts:   0%|          | 0/10 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "stockout_model_uri = \"models:/workspace.retail_capstone.stockout_risk_classifier@champion\"\n",
    "\n",
    "stockout_model = mlflow.pyfunc.load_model(stockout_model_uri)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "29097da9-5298-4760-bd47-cd25089ffd32",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['High Risk' 'Low Risk']\n"
     ]
    }
   ],
   "source": [
    "stockout_sample = pd.DataFrame([\n",
    "    {\n",
    "        \"available_stock\": 20.0,\n",
    "        \"avg_daily_sales\": 8.5,\n",
    "        \"days_of_inventory_remaining\": 2.35,\n",
    "        \"lead_time_days\": 7.0,\n",
    "        \"reliability_score\": 0.91,\n",
    "        \"category\": \"Home Decor\",\n",
    "        \"warehouse_id\": \"WH001\"\n",
    "    },\n",
    "    {\n",
    "        \"available_stock\": 200.0,\n",
    "        \"avg_daily_sales\": 3.0,\n",
    "        \"days_of_inventory_remaining\": 66.67,\n",
    "        \"lead_time_days\": 7.0,\n",
    "        \"reliability_score\": 0.94,\n",
    "        \"category\": \"Home Decor\",\n",
    "        \"warehouse_id\": \"WH001\"\n",
    "    }\n",
    "])\n",
    "\n",
    "stockout_predictions = stockout_model.predict(stockout_sample)\n",
    "\n",
    "print(stockout_predictions)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "0d0c5590-c87b-4087-9880-5a1a0fd821e3",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "application/vnd.jupyter.widget-view+json": {
       "model_id": "5b40298ffcfc437684dd654bd98c2d43",
       "version_major": 2,
       "version_minor": 0
      },
      "text/plain": [
       "Downloading artifacts:   0%|          | 0/10 [00:00<?, ?it/s]"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "reorder_model_uri = \"models:/workspace.retail_capstone.reorder_flag_classifier@champion\"\n",
    "\n",
    "reorder_model = mlflow.pyfunc.load_model(reorder_model_uri)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "baa8ed36-773e-4248-af54-d506f55d2b3d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['Reorder Needed' 'No Reorder Needed']\n"
     ]
    }
   ],
   "source": [
    "reorder_sample = pd.DataFrame([\n",
    "    {\n",
    "        \"available_stock\": 30.0,\n",
    "        \"reorder_level\": 100.0,\n",
    "        \"reorder_quantity\": 300.0,\n",
    "        \"lead_time_days\": 7.0,\n",
    "        \"category\": \"Home Decor\",\n",
    "        \"warehouse_id\": \"WH001\"\n",
    "    },\n",
    "    {\n",
    "        \"available_stock\": 250.0,\n",
    "        \"reorder_level\": 100.0,\n",
    "        \"reorder_quantity\": 300.0,\n",
    "        \"lead_time_days\": 7.0,\n",
    "        \"category\": \"Home Decor\",\n",
    "        \"warehouse_id\": \"WH001\"\n",
    "    }\n",
    "])\n",
    "\n",
    "reorder_predictions = reorder_model.predict(reorder_sample)\n",
    "\n",
    "print(reorder_predictions)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e8c7ae16-aca5-43de-99af-aa5e055f339c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Serving Test Summary\n",
    "\n",
    "The champion models were loaded from Unity Catalog Model Registry using model aliases.\n",
    "\n",
    "Model URIs:\n",
    "\n",
    "- `models:/workspace.retail_capstone.stockout_risk_classifier@champion`\n",
    "- `models:/workspace.retail_capstone.reorder_flag_classifier@champion`\n",
    "\n",
    "Sample input data was passed to both models.\n",
    "\n",
    "The stockout model predicts risk level.\n",
    "\n",
    "The reorder model predicts whether reorder is needed.\n",
    "\n",
    "This confirms that the registered champion models can be loaded and used for inference."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "13cf78cd-372a-46ee-9806-bd8d67d6eead",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Databricks Model Serving Endpoint Test\n",
    "\n",
    "This section tests the deployed Databricks Model Serving endpoint for the stockout risk classifier.\n",
    "\n",
    "Endpoint name:\n",
    "\n",
    "`stockout-risk-classifier-endpoint`"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "ac8ea8ab-d0ab-4590-80fc-536bd08d9e0c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import requests\n",
    "import json\n",
    "import os"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "aaafc5ce-9893-49e2-a811-3fef2a6c1f28",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Status code: 200\nResponse:\n{\"predictions\": [\"High Risk\", \"Low Risk\"]}\n"
     ]
    }
   ],
   "source": [
    "DATABRICKS_HOST = \"https://<your-databricks-workspace-url>\"\n",
    "DATABRICKS_TOKEN = \"<your-databricks-personal-access-token>\"\n",
    "\n",
    "endpoint_name = \"stockout-risk-classifier-endpoint\"\n",
    "\n",
    "url = f\"{DATABRICKS_HOST}/serving-endpoints/{endpoint_name}/invocations\"\n",
    "\n",
    "headers = {\n",
    "    \"Authorization\": f\"Bearer {DATABRICKS_TOKEN}\",\n",
    "    \"Content-Type\": \"application/json\"\n",
    "}\n",
    "\n",
    "payload = {\n",
    "    \"dataframe_records\": [\n",
    "        {\n",
    "            \"available_stock\": 20,\n",
    "            \"avg_daily_sales\": 8.5,\n",
    "            \"days_of_inventory_remaining\": 2.35,\n",
    "            \"lead_time_days\": 7,\n",
    "            \"reliability_score\": 0.91,\n",
    "            \"category\": \"Home Decor\",\n",
    "            \"warehouse_id\": \"WH001\"\n",
    "        },\n",
    "        {\n",
    "            \"available_stock\": 200,\n",
    "            \"avg_daily_sales\": 3.0,\n",
    "            \"days_of_inventory_remaining\": 66.67,\n",
    "            \"lead_time_days\": 7,\n",
    "            \"reliability_score\": 0.94,\n",
    "            \"category\": \"Home Decor\",\n",
    "            \"warehouse_id\": \"WH001\"\n",
    "        }\n",
    "    ]\n",
    "}\n",
    "\n",
    "response = requests.post(url, headers=headers, data=json.dumps(payload))\n",
    "\n",
    "print(\"Status code:\", response.status_code)\n",
    "print(\"Response:\")\n",
    "print(response.text)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "dc09a0c0-b56b-4b4f-ad62-a15c71e0b538",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Example curl Request\n",
    "\n",
    "The endpoint can also be tested using curl.\n",
    "\n",
    "Replace `<your-databricks-workspace-url>` and `<your-token>` before running.\n",
    "\n",
    "```bash\n",
    "curl -X POST https://<your-databricks-workspace-url>/serving-endpoints/stockout-risk-classifier-endpoint/invocations \\\n",
    "  -H \"Authorization: Bearer <your-token>\" \\\n",
    "  -H \"Content-Type: application/json\" \\\n",
    "  -d '{\n",
    "    \"dataframe_records\": [\n",
    "      {\n",
    "        \"available_stock\": 20,\n",
    "        \"avg_daily_sales\": 8.5,\n",
    "        \"days_of_inventory_remaining\": 2.35,\n",
    "        \"lead_time_days\": 7,\n",
    "        \"reliability_score\": 0.91,\n",
    "        \"category\": \"Home Decor\",\n",
    "        \"warehouse_id\": \"WH001\"\n",
    "      }\n",
    "    ]\n",
    "  }'"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "5"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "05_model_serving_test",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}