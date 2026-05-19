{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "91b40edd-cd47-460c-88b8-cb909a074ffc",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 08 GenAI RAG Evaluation\n",
    "\n",
    "This notebook evaluates the inventory RAG retrieval layer.\n",
    "\n",
    "The evaluation uses business queries and checks whether the retrieved inventory documents contain expected relevant signals."
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
     "nuid": "37ee8141-7649-4046-9f29-80a95c769191",
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
      "+-----------------+----------------+\n|current_catalog()|current_schema()|\n+-----------------+----------------+\n|        workspace| retail_capstone|\n+-----------------+----------------+\n\n"
     ]
    }
   ],
   "source": [
    "spark.sql(\"USE CATALOG workspace\")\n",
    "spark.sql(\"USE SCHEMA retail_capstone\")\n",
    "\n",
    "spark.sql(\"SELECT current_catalog(), current_schema()\").show()"
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
     "nuid": "c0638267-cba6-485e-a10a-e7f40f2e388e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import numpy as np\n",
    "\n",
    "from pyspark.sql.functions import col, lower, lit, when, round"
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
     "nuid": "f99653ab-c1df-47f8-b2dc-f9990c8b20b3",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>85123A_WH001</td><td>85123A</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>High Risk</td><td>No Reorder Needed</td><td>0.606</td><td>Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84879_WH001</td><td>84879</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>High Risk</td><td>No Reorder Needed</td><td>0.606</td><td>Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0.</td></tr><tr><td>71053_WH001</td><td>71053</td><td>WHITE METAL LANTERN</td><td>Medium Risk</td><td>Reorder Needed</td><td>0.756</td><td>Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0.</td></tr><tr><td>21730_WH001</td><td>21730</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>Low Risk</td><td>No Reorder Needed</td><td>0.251</td><td>Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0.</td></tr><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr><tr><td>22632_WH003</td><td>22632</td><td>HAND WARMER RED POLKA DOT</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>Product 22632, HAND WARMER RED POLKA DOT, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 50 units. Current stock is 75 units. Average daily sales is 43.69 units. Days of inventory remaining is 1.14. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 82.5.</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "84029G_WH002",
         "84029G",
         "KNITTED UNION FLAG HOT WATER BOTTLE",
         "High Risk",
         "Reorder Needed",
         0.909,
         "Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5."
        ],
        [
         "85123A_WH001",
         "85123A",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "High Risk",
         "No Reorder Needed",
         0.606,
         "Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5."
        ],
        [
         "84029E_WH002",
         "84029E",
         "RED WOOLLY HOTTIE WHITE HEART",
         "High Risk",
         "Reorder Needed",
         0.909,
         "Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0."
        ],
        [
         "84879_WH001",
         "84879",
         "ASSORTED COLOUR BIRD ORNAMENT",
         "High Risk",
         "No Reorder Needed",
         0.606,
         "Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0."
        ],
        [
         "71053_WH001",
         "71053",
         "WHITE METAL LANTERN",
         "Medium Risk",
         "Reorder Needed",
         0.756,
         "Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0."
        ],
        [
         "21730_WH001",
         "21730",
         "GLASS STAR FROSTED T-LIGHT HOLDER",
         "Low Risk",
         "No Reorder Needed",
         0.251,
         "Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0."
        ],
        [
         "22752_WH003",
         "22752",
         "SET 7 BABUSHKA NESTING BOXES",
         "High Risk",
         "Reorder Needed",
         0.914,
         "Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0."
        ],
        [
         "84406B_WH002",
         "84406B",
         "CREAM CUPID HEARTS COAT HANGER",
         "High Risk",
         "Reorder Needed",
         0.912,
         "Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25."
        ],
        [
         "22633_WH003",
         "22633",
         "HAND WARMER UNION JACK",
         "High Risk",
         "Reorder Needed",
         0.909,
         "Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0."
        ],
        [
         "22632_WH003",
         "22632",
         "HAND WARMER RED POLKA DOT",
         "High Risk",
         "Reorder Needed",
         0.909,
         "Product 22632, HAND WARMER RED POLKA DOT, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 50 units. Current stock is 75 units. Average daily sales is 43.69 units. Days of inventory remaining is 1.14. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 82.5."
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "document_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "stock_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "stockout_risk_level",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "reorder_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "business_priority_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "rag_document_text",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "rag_df = spark.table(\"workspace.retail_capstone.inventory_rag_documents\")\n",
    "\n",
    "display(\n",
    "    rag_df.select(\n",
    "        \"document_id\",\n",
    "        \"stock_code\",\n",
    "        \"product_name\",\n",
    "        \"stockout_risk_level\",\n",
    "        \"reorder_flag\",\n",
    "        \"business_priority_score\",\n",
    "        \"rag_document_text\"\n",
    "    )\n",
    ")"
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
     "nuid": "dd9ceb75-3766-40b1-bfc9-521113f216a1",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "def retrieve_inventory_context(query: str, top_k: int = 5):\n",
    "    query_lower = query.lower()\n",
    "\n",
    "    result_df = rag_df.withColumn(\n",
    "        \"query\",\n",
    "        lit(query_lower)\n",
    "    ).withColumn(\n",
    "        \"retrieval_score\",\n",
    "        when(lower(col(\"rag_document_text\")).contains(query_lower), 1.0)\n",
    "        .when(lower(col(\"stockout_risk_level\")).contains(query_lower), 0.9)\n",
    "        .when(lower(col(\"reorder_flag\")).contains(query_lower), 0.9)\n",
    "        .when(lower(col(\"product_name\")).contains(query_lower), 0.8)\n",
    "        .when(lower(col(\"category\")).contains(query_lower), 0.7)\n",
    "        .when(lower(col(\"supplier_name\")).contains(query_lower), 0.7)\n",
    "        .otherwise(0.1)\n",
    "    ).withColumn(\n",
    "        \"final_score\",\n",
    "        round(\n",
    "            col(\"retrieval_score\") * (lit(0.6) + lit(0.4) * col(\"business_priority_score\")),\n",
    "            4\n",
    "        )\n",
    "    ).orderBy(\n",
    "        col(\"final_score\").desc(),\n",
    "        col(\"business_priority_score\").desc()\n",
    "    )\n",
    "\n",
    "    return result_df.select(\n",
    "        \"document_id\",\n",
    "        \"stock_code\",\n",
    "        \"product_name\",\n",
    "        \"warehouse_id\",\n",
    "        \"stockout_risk_level\",\n",
    "        \"reorder_flag\",\n",
    "        \"business_priority_score\",\n",
    "        \"retrieval_score\",\n",
    "        \"final_score\",\n",
    "        \"rag_document_text\"\n",
    "    ).limit(top_k)"
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
     "nuid": "fa6f1df8-f439-40e5-9fc2-030991b325cb",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>query</th><th>expected_terms</th></tr></thead><tbody><tr><td>Which products are high stockout risk?</td><td>List(High Risk)</td></tr><tr><td>Which products should be reordered?</td><td>List(Reorder Needed)</td></tr><tr><td>Which products have low available stock?</td><td>List(Available stock)</td></tr><tr><td>Which suppliers have long lead times?</td><td>List(lead time)</td></tr><tr><td>Which products have high sales velocity?</td><td>List(Average daily sales)</td></tr><tr><td>Show products with low stock and high sales velocity</td><td>List(Available stock, Average daily sales)</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Which products are high stockout risk?",
         [
          "High Risk"
         ]
        ],
        [
         "Which products should be reordered?",
         [
          "Reorder Needed"
         ]
        ],
        [
         "Which products have low available stock?",
         [
          "Available stock"
         ]
        ],
        [
         "Which suppliers have long lead times?",
         [
          "lead time"
         ]
        ],
        [
         "Which products have high sales velocity?",
         [
          "Average daily sales"
         ]
        ],
        [
         "Show products with low stock and high sales velocity",
         [
          "Available stock",
          "Average daily sales"
         ]
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "query",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "expected_terms",
         "type": "{\"containsNull\":true,\"elementType\":\"string\",\"type\":\"array\"}"
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "evaluation_queries = [\n",
    "    {\n",
    "        \"query\": \"Which products are high stockout risk?\",\n",
    "        \"expected_terms\": [\"High Risk\"]\n",
    "    },\n",
    "    {\n",
    "        \"query\": \"Which products should be reordered?\",\n",
    "        \"expected_terms\": [\"Reorder Needed\"]\n",
    "    },\n",
    "    {\n",
    "        \"query\": \"Which products have low available stock?\",\n",
    "        \"expected_terms\": [\"Available stock\"]\n",
    "    },\n",
    "    {\n",
    "        \"query\": \"Which suppliers have long lead times?\",\n",
    "        \"expected_terms\": [\"lead time\"]\n",
    "    },\n",
    "    {\n",
    "        \"query\": \"Which products have high sales velocity?\",\n",
    "        \"expected_terms\": [\"Average daily sales\"]\n",
    "    },\n",
    "    {\n",
    "        \"query\": \"Show products with low stock and high sales velocity\",\n",
    "        \"expected_terms\": [\"Available stock\", \"Average daily sales\"]\n",
    "    }\n",
    "]\n",
    "\n",
    "evaluation_pd = pd.DataFrame(evaluation_queries)\n",
    "display(evaluation_pd)"
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
     "nuid": "9db872a5-095f-4be0-8564-a8ce8ad52142",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "def is_relevant(document_text: str, expected_terms: list) -> int:\n",
    "    text_lower = document_text.lower()\n",
    "\n",
    "    for term in expected_terms:\n",
    "        if term.lower() in text_lower:\n",
    "            return 1\n",
    "\n",
    "    return 0"
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
     "nuid": "aafe10ba-9d09-429b-a272-35d28c741330",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "def precision_at_k(relevance_list, k):\n",
    "    top_k = relevance_list[:k]\n",
    "    if k == 0:\n",
    "        return 0.0\n",
    "    return sum(top_k) / k\n",
    "\n",
    "\n",
    "def recall_at_k(relevance_list, total_relevant, k):\n",
    "    if total_relevant == 0:\n",
    "        return 0.0\n",
    "    top_k = relevance_list[:k]\n",
    "    return sum(top_k) / total_relevant\n",
    "\n",
    "\n",
    "def reciprocal_rank(relevance_list):\n",
    "    for idx, rel in enumerate(relevance_list):\n",
    "        if rel == 1:\n",
    "            return 1 / (idx + 1)\n",
    "    return 0.0\n",
    "\n",
    "\n",
    "def dcg_at_k(relevance_list, k):\n",
    "    score = 0.0\n",
    "    for idx, rel in enumerate(relevance_list[:k]):\n",
    "        score += rel / np.log2(idx + 2)\n",
    "    return score\n",
    "\n",
    "\n",
    "def ndcg_at_k(relevance_list, k):\n",
    "    dcg = dcg_at_k(relevance_list, k)\n",
    "    ideal_relevance = sorted(relevance_list, reverse=True)\n",
    "    idcg = dcg_at_k(ideal_relevance, k)\n",
    "\n",
    "    if idcg == 0:\n",
    "        return 0.0\n",
    "\n",
    "    return dcg / idcg"
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
     "nuid": "c6a3b8d8-9bee-4457-9e49-a779c485c3ce",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>query</th><th>expected_terms</th><th>mrr</th><th>precision_at_1</th><th>precision_at_3</th><th>precision_at_5</th><th>recall_at_3</th><th>recall_at_5</th><th>ndcg_at_3</th><th>ndcg_at_5</th></tr></thead><tbody><tr><td>Which products are high stockout risk?</td><td>High Risk</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr><tr><td>Which products should be reordered?</td><td>Reorder Needed</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr><tr><td>Which products have low available stock?</td><td>Available stock</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr><tr><td>Which suppliers have long lead times?</td><td>lead time</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr><tr><td>Which products have high sales velocity?</td><td>Average daily sales</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr><tr><td>Show products with low stock and high sales velocity</td><td>Available stock, Average daily sales</td><td>1.0</td><td>1.0</td><td>1.0</td><td>1.0</td><td>0.6</td><td>1.0</td><td>1.0</td><td>1.0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "Which products are high stockout risk?",
         "High Risk",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ],
        [
         "Which products should be reordered?",
         "Reorder Needed",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ],
        [
         "Which products have low available stock?",
         "Available stock",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ],
        [
         "Which suppliers have long lead times?",
         "lead time",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ],
        [
         "Which products have high sales velocity?",
         "Average daily sales",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ],
        [
         "Show products with low stock and high sales velocity",
         "Available stock, Average daily sales",
         1.0,
         1.0,
         1.0,
         1.0,
         0.6,
         1.0,
         1.0,
         1.0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "query",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "expected_terms",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "mrr",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "precision_at_1",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "precision_at_3",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "precision_at_5",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "recall_at_3",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "recall_at_5",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "ndcg_at_3",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "ndcg_at_5",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "evaluation_results = []\n",
    "\n",
    "for item in evaluation_queries:\n",
    "    query = item[\"query\"]\n",
    "    expected_terms = item[\"expected_terms\"]\n",
    "\n",
    "    retrieved_pd = retrieve_inventory_context(query, top_k=5).toPandas()\n",
    "\n",
    "    relevance_list = [\n",
    "        is_relevant(text, expected_terms)\n",
    "        for text in retrieved_pd[\"rag_document_text\"].tolist()\n",
    "    ]\n",
    "\n",
    "    total_relevant = max(sum(relevance_list), 1)\n",
    "\n",
    "    result = {\n",
    "        \"query\": query,\n",
    "        \"expected_terms\": \", \".join(expected_terms),\n",
    "        \"mrr\": reciprocal_rank(relevance_list),\n",
    "        \"precision_at_1\": precision_at_k(relevance_list, 1),\n",
    "        \"precision_at_3\": precision_at_k(relevance_list, 3),\n",
    "        \"precision_at_5\": precision_at_k(relevance_list, 5),\n",
    "        \"recall_at_3\": recall_at_k(relevance_list, total_relevant, 3),\n",
    "        \"recall_at_5\": recall_at_k(relevance_list, total_relevant, 5),\n",
    "        \"ndcg_at_3\": ndcg_at_k(relevance_list, 3),\n",
    "        \"ndcg_at_5\": ndcg_at_k(relevance_list, 5)\n",
    "    }\n",
    "\n",
    "    evaluation_results.append(result)\n",
    "\n",
    "evaluation_results_pd = pd.DataFrame(evaluation_results)\n",
    "\n",
    "display(evaluation_results_pd)"
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
     "nuid": "917c48e9-8f5c-400d-ad53-2eca0daf9d1b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>metric</th><th>average_value</th></tr></thead><tbody><tr><td>mrr</td><td>1.0</td></tr><tr><td>precision_at_1</td><td>1.0</td></tr><tr><td>precision_at_3</td><td>1.0</td></tr><tr><td>precision_at_5</td><td>1.0</td></tr><tr><td>recall_at_3</td><td>0.6</td></tr><tr><td>recall_at_5</td><td>1.0</td></tr><tr><td>ndcg_at_3</td><td>1.0</td></tr><tr><td>ndcg_at_5</td><td>1.0</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "mrr",
         1.0
        ],
        [
         "precision_at_1",
         1.0
        ],
        [
         "precision_at_3",
         1.0
        ],
        [
         "precision_at_5",
         1.0
        ],
        [
         "recall_at_3",
         0.6
        ],
        [
         "recall_at_5",
         1.0
        ],
        [
         "ndcg_at_3",
         1.0
        ],
        [
         "ndcg_at_5",
         1.0
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "metric",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "average_value",
         "type": "\"double\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "summary_metrics = evaluation_results_pd[[\n",
    "    \"mrr\",\n",
    "    \"precision_at_1\",\n",
    "    \"precision_at_3\",\n",
    "    \"precision_at_5\",\n",
    "    \"recall_at_3\",\n",
    "    \"recall_at_5\",\n",
    "    \"ndcg_at_3\",\n",
    "    \"ndcg_at_5\"\n",
    "]].mean().reset_index()\n",
    "\n",
    "summary_metrics.columns = [\"metric\", \"average_value\"]\n",
    "\n",
    "display(summary_metrics)"
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
     "nuid": "4bf03ecc-47da-47cd-aeb8-110d3f47b843",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "image/png": "iVBORw0KGgoAAAANSUhEUgAAA04AAAIZCAYAAACRXG96AAAAOnRFWHRTb2Z0d2FyZQBNYXRwbG90bGliIHZlcnNpb24zLjEwLjAsIGh0dHBzOi8vbWF0cGxvdGxpYi5vcmcvlHJYcgAAAAlwSFlzAAAPYQAAD2EBqD+naQAAb+RJREFUeJzt3Xd8jff///HnyUbEjtUg9l5RqlYVpdRoaY1aMYryUdRuS9EaVau1t7ZUFNVNzdqjYtcm0hqxEzuRvH9/+OV8cySchCQn4XG/3dzaXOu88srJlet5jfexGGOMAAAAAACP5OToAgAAAAAgpSM4AQAAAIAdBCcAAAAAsIPgBAAAAAB2EJwAAAAAwA6CEwAAAADYQXACAAAAADsITgAAAABgB8EJAAAAAOwgOAEAUh2LxaJPP/3U0WUoKChIFotF8+fPd3Qpj5QvXz61b9/eIa+dGvqT3Nq3b698+fI5ugwAT4DgBOCJzJ8/XxaLRX///bejS0mQrVu36tNPP9X169cdXUosr7zyiiwWi/VfmjRpVLp0aU2cOFFRUVGPXK9ixYqyWCyaNm3aY7e/adMmvfPOO8qdO7fc3NyUIUMGVapUScOHD1dISIjd+j799FOb+lxdXZUvXz717NnzifuZkn8eiWnDhg02vXv43+LFix1d4lNZtGiRJk6c6OgybLRv314Wi0VeXl66c+dOrPnHjx+39v/LL79M8PZv376tTz/9VBs2bEiEagGkBi6OLgAAktPWrVs1bNgwtW/fXhkzZnR0ObG88MILGjVqlCTp8uXLWrRokXr37q1Lly7p888/j7X88ePHtWvXLuXLl08LFy5Ut27d4tzukCFDNGLECOXPn1/t27dX/vz5dffuXe3evVvjxo3TggULdPLkyXjVOG3aNHl6eurWrVtau3atvv76awUGBmrz5s0J/n6f9Odx584dubikvj9hPXv21IsvvhhreuXKlR1QTeJZtGiRDh48qF69etlMz5s3r+7cuSNXV1eH1OXi4qLbt2/rl19+0TvvvGMzb+HChfLw8NDdu3efaNu3b9/WsGHDJD046RFfs2bNeuyJEAApV+r7qwMAKYwxRnfv3lWaNGmeelsZMmRQ69atrV937dpVRYsW1ddff63hw4fL2dnZZvnvvvtO3t7eGjdunJo1a6agoKBYtwEFBARoxIgReuedd/Ttt9/Kzc3NZv6ECRM0YcKEeNfYrFkzZc2aVZLUpUsXtWjRQgEBAdq5c6cqVqyYwO84/qKiohQeHi4PDw95eHgk2eskpWrVqqlZs2aOLiPZWCwWh/6s3N3dVaVKFX3//fexgtOiRYvUoEEDLVu2LFlquXXrltKlS+ewEAng6XGrHoBE0759e3l6eurs2bNq0qSJPD09lS1bNvXt21eRkZGSpIiICGXOnFn+/v6x1g8LC5OHh4f69u1rnXbv3j0NHTpUBQsWlLu7u3x8fNS/f3/du3fPZl2LxaIePXpoxYoVKlmypNzd3VWiRAmtXLnSusynn36qfv36SZJ8fX2tt+kEBQVJku7fv68RI0aoQIECcnd3V758+TR48OBYr5UvXz698cYbWrVqlSpUqKA0adJoxowZqlGjhsqUKRNnb4oUKaK6desmuKceHh568cUXdePGDV28eDHW/EWLFqlZs2Z64403lCFDBi1atCjWMkOGDFHWrFk1Z86cWKFJehDWnuZ5oWrVqklSrCtWO3bsUL169ZQhQwalTZtWNWrU0JYtW6zz7f08on+mCxcuVIkSJeTu7m79ecb1jNPZs2fVoUMHZc+e3frznzt3rnV+SEiIXFxcrFcJYjp69KgsFosmT54sSbp69ar69u2rUqVKydPTU15eXnr99de1b9++J+5TfJQsWVI1a9aMNT0qKkq5c+e2CV1ffvmlXn75ZWXJkkVp0qSRn5+fli5davc1om+5fFj07bfR/Zekn376SQ0aNFCuXLnk7u6uAgUKaMSIEdbfZ+nB1ZbffvtNZ86csf4Mo8P7o55xWrdunapVq6Z06dIpY8aMaty4sQ4fPhxnnSdOnLBekcyQIYP8/f11+/Ztu99ntFatWumPP/6wuR10165dOn78uFq1ahXnOtevX1evXr3k4+Mjd3d3FSxYUGPGjLFeKQoKClK2bNkkScOGDbN+39Hvyeh94cmTJ1W/fn2lT59e7777rnXewyc3oqKiNGnSJJUqVUoeHh7Kli2b6tWrZ3Mr9OrVq1W1alVlzJhRnp6eKlKkiAYPHhzvPgB4elxxApCoIiMjVbduXVWqVElffvml1qxZo3HjxqlAgQLq1q2bXF1d9eabb2r58uWaMWOGzYH8ihUrdO/ePbVo0ULSg4OJRo0aafPmzXrvvfdUrFgxHThwQBMmTNCxY8e0YsUKm9fevHmzli9frvfff1/p06fXV199paZNmyo4OFhZsmTRW2+9pWPHjun777/XhAkTrFdNog+AOnXqpAULFqhZs2b68MMPtWPHDo0aNUqHDx/Wjz/+aPNaR48eVcuWLdWlSxd17txZRYoUkaenpzp37qyDBw+qZMmS1mV37dqlY8eO6eOPP36inkYffD58K9uOHTt04sQJzZs3T25ubnrrrbe0cOFCm4OpY8eO6dixY+rUqZM8PT2f6PXjU58kZcqUyTpt3bp1ev311+Xn56ehQ4fKyclJ8+bN06uvvqpNmzapYsWKdn8e0dtZsmSJevTooaxZsz7yofqQkBC99NJL1rCVLVs2/fHHH+rYsaPCwsLUq1cvZc+eXTVq1NCSJUs0dOhQm/UDAgLk7Oyst99+W5J06tQprVixQm+//bZ8fX0VEhJiDcf//POPcuXK9US9unHjhi5fvhxrepYsWWSxWNS8eXN9+umnunDhgnLkyGGdv3nzZp07d876uyFJkyZNUqNGjfTuu+8qPDxcixcv1ttvv61ff/1VDRo0eKL6HjZ//nx5enqqT58+8vT01Lp16zRkyBCFhYVp7NixkqSPPvpIoaGh+u+//6xXLh/3XluzZo1ef/115c+fX59++qnu3Lmjr7/+WlWqVFFgYGCsn/E777wjX19fjRo1SoGBgZo9e7a8vb01ZsyYeH0Pb731lrp27arly5erQ4cOkh6ccChatKjKly8fa/nbt2+rRo0aOnv2rLp06aI8efJo69atGjRokM6fP6+JEycqW7ZsmjZtmrp166Y333xTb731liSpdOnS1u3cv39fdevWVdWqVfXll18qbdq0j6yxY8eOmj9/vl5//XV16tRJ9+/f16ZNm7R9+3ZVqFBBhw4d0htvvKHSpUtr+PDhcnd314kTJ2xORABIBgYAnsC8efOMJLNr1y7rtHbt2hlJZvjw4TbLlitXzvj5+Vm/XrVqlZFkfvnlF5vl6tevb/Lnz2/9+ttvvzVOTk5m06ZNNstNnz7dSDJbtmyxTpNk3NzczIkTJ6zT9u3bZySZr7/+2jpt7NixRpI5ffq0zTb37t1rJJlOnTrZTO/bt6+RZNatW2edljdvXiPJrFy50mbZ69evGw8PDzNgwACb6T179jTp0qUzN2/eNI9To0YNU7RoUXPp0iVz6dIlc+TIEdOvXz8jyTRo0CDW8j169DA+Pj4mKirKGGPMn3/+aSSZPXv2WJf56aefjCQzceJEm3WjoqKsrxP9LyIi4rH1DR061EgyR48eNZcuXTJBQUFm7ty5Jk2aNCZbtmzm1q1b1m0XKlTI1K1b11qbMcbcvn3b+Pr6mjp16linPernYcyDn6mTk5M5dOhQnPOGDh1q/bpjx44mZ86c5vLlyzbLtWjRwmTIkMHcvn3bGGPMjBkzjCRz4MABm+WKFy9uXn31VevXd+/eNZGRkTbLnD592ri7u9u8v0+fPm0kmXnz5j2iaw+sX7/eSHrkv/PnzxtjjDl69Gis96wxxrz//vvG09PT+n0YY2z+3xhjwsPDTcmSJW2+D2MevF/btWtn/Tr65/iw6N/pmD+Lh1/DGGO6dOli0qZNa+7evWud1qBBA5M3b95Yy8bVn7Jlyxpvb29z5coV67R9+/YZJycn07Zt21h1dujQwWabb775psmSJUus13pYu3btTLp06YwxxjRr1szUqlXLGGNMZGSkyZEjhxk2bJi1vrFjx1rXGzFihEmXLp05duyYzfYGDhxonJ2dTXBwsDHGmEuXLsV6H8Z8bUlm4MCBcc6L2at169YZSaZnz56xlo3+/ZkwYYKRZC5dumT3+waQdLhVD0Ci69q1q83X1apV06lTp6xfv/rqq8qaNasCAgKs065du6bVq1erefPm1mk//PCDihUrpqJFi+ry5cvWf6+++qokaf369TavU7t2bRUoUMD6denSpeXl5WXz2o/y+++/S5L69OljM/3DDz+UJP3222820319fWPdepchQwY1btxY33//vYwxkh5cgQsICFCTJk2ULl06u3UcOXJE2bJlU7Zs2VS0aFGNHTtWjRo1inWr0/379xUQEKDmzZtbb7t69dVX5e3trYULF1qXCwsLkxT7CkBoaKj1daL/7d2712590oPbDrNly6Z8+fKpQ4cOKliwoP744w/rGfW9e/dab4O6cuWK9ed269Yt1apVSxs3boz3w/E1atRQ8eLFH7uMMUbLli1Tw4YNZYyxea/UrVtXoaGhCgwMlPTg6oOLi4vNe+/gwYP6559/bN577u7ucnJ68CcyMjJSV65csd4eFb2tJzFkyBCtXr061r/MmTNLkgoXLqyyZcva1BcZGamlS5eqYcOGNs/Rxfz/a9euKTQ0VNWqVXuq+h4W8zWir5ZVq1ZNt2/f1pEjRxK8vfPnz2vv3r1q37699XuWHvyu1qlTx/p7GFNc+5MrV65Y39vx0apVK23YsEEXLlzQunXrdOHChUfepvfDDz+oWrVqypQpk817qXbt2oqMjNTGjRvj/bqPGqwlpmXLlsliscS6CirJ+rsdfbX5p59+YmAJwIG4VQ9Aooq+Pz+mTJky6dq1a9avXVxc1LRpUy1atEj37t2Tu7u7li9froiICJuD1+PHj+vw4cOxthft4Wd+8uTJE2uZh1/7Uc6cOSMnJycVLFjQZnqOHDmUMWNGnTlzxma6r69vnNtp27atAgICtGnTJlWvXl1r1qxRSEiI2rRpY7cG6cHzU9Gjbp08eVKff/65Ll26FOsB+z///FOXLl1SxYoVdeLECev0mjVr6vvvv9eYMWPk5OSk9OnTS5Ju3rxps76np6dWr15t3Vb0bVfxsWzZMnl5eenSpUv66quvdPr0aZsD7OPHj0uS2rVr98hthIaG2tza9yiP6nNMly5d0vXr1zVz5kzNnDkzzmWi3ytZs2ZVrVq1tGTJEo0YMULSg9v0XFxcrLdbSf/3zMnUqVN1+vRpm2d6smTJYremRylVqpRq16792GWaN2+uwYMH6+zZs8qdO7c2bNigixcv2vxuSNKvv/6qzz77THv37rV5Di+u55ee1KFDh/Txxx9r3bp1sYJKaGhogrcX/XtUpEiRWPOKFSumVatWWQdRiPbw73X0++batWvy8vKK1+tGP2cUEBCgvXv36sUXX1TBggVtnueKdvz4ce3fvz/e+51HcXFx0QsvvGB3uZMnTypXrlw2QfJhzZs31+zZs9WpUycNHDhQtWrV0ltvvaVmzZpZAz6ApEdwApCoHh717VFatGihGTNm6I8//lCTJk20ZMkSFS1a1GZwhaioKJUqVUrjx4+Pcxs+Pj7xeu3oqz/xEd+DzkeNoFe3bl1lz55d3333napXr67vvvtOOXLksHuwHC1dunQ2y1apUkXly5fX4MGD9dVXX1mnR19VeniksGh//fWXatasqaJFi0p6cFUlJhcXF+vr/Pfff/GqLVr16tWtzyM1bNhQpUqV0rvvvqvdu3fLycnJekZ87NixKlu2bJzbiO/zVvEZqTD69Vq3bv3IsBbz2ZMWLVrI399fe/fuVdmyZbVkyRLVqlXL+j1J0siRI/XJJ5+oQ4cOGjFihDJnziwnJyf16tUryc/4N2/eXIMGDdIPP/ygXr16acmSJcqQIYPq1atnXWbTpk1q1KiRqlevrqlTpypnzpxydXXVvHnz4hwgJKZHvcdjhkPpwQAJNWrUkJeXl4YPH64CBQrIw8NDgYGBGjBgQLJd+UiM32t3d3e99dZbWrBggU6dOvXYwVCioqJUp04d9e/fP875hQsXjvdrJlaoSZMmjTZu3Kj169frt99+08qVKxUQEKBXX31Vf/75Z7z3uwCeDsEJgENUr15dOXPmVEBAgKpWrap169bpo48+slmmQIEC2rdvn2rVqpVoZ9EftZ28efMqKipKx48fV7FixazTQ0JCdP36deXNmzde23d2dlarVq00f/58jRkzRitWrFDnzp2f+MCmdOnSat26tWbMmKG+ffsqT548unXrln766Sc1b948zqGte/bsqYULF6pmzZoqUqSIChUqpBUrVmjixInxul0wITw9PTV06FD5+/tryZIlatGihfV2SS8vL7uBMTF+rtmyZVP69OkVGRkZr4DapEkTdenSxXo73LFjxzRo0CCbZZYuXaqaNWtqzpw5NtOvX79uE7CSgq+vrypWrKiAgAD16NFDy5cvV5MmTeTu7m5dZtmyZfLw8NCqVatsps+bN8/u9qOv2Fy/ft1mwJGHr6pu2LBBV65c0fLly1W9enXr9NOnT8faZnx/jtG/R0ePHo0178iRI8qaNWuiv0ejtWrVSnPnzpWTk5PNIBsPK1CggG7evJks793o11u1apWuXr362KtOTk5OqlWrlmrVqqXx48dr5MiR+uijj7R+/fp4n5gB8HS4vgvAIZycnNSsWTP98ssv+vbbb3X//v1YtyK98847Onv2rGbNmhVr/Tt37ujWrVsJft3og7KYQxNLD27lkaSJEyfaTI++2pWQUcratGmja9euqUuXLrp586bN5zI9if79+ysiIsJay48//qhbt26pe/fuatasWax/b7zxhpYtW2a9fevTTz/V5cuX1blzZ0VERMTafkLO3Mfl3Xff1QsvvGAd5czPz08FChTQl19+GesWQenBrXXRHvXzSAhnZ2c1bdpUy5Yti3Vl7eHXkx48L1K3bl0tWbJEixcvlpubm5o0aRJrmw/35YcfftDZs2efuM6EaN68ubZv3665c+fq8uXLsX43nJ2dZbFYbK4SBQUFxRppMi7RwTbmszq3bt3SggULYr2GZPv+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XS1atEh58uRJ9NcLDw+XJOXIkUPu7u6Jvn0AQOpAcAIApCrly5eXj4+Pli9fbp22fPly5cmTR+XKlbNOi4qK0qhRo+Tr66s0adKoTJkyWrp0qSQpKChINWvWlCRlypTJ5krVK6+8oh49eqhXr17KmjWr6tatKyn2rXr//fefWrZsqcyZMytdunSqUKGCduzYkcTfPQDAUVwcXQAAAAnVoUMHzZs3T++++64kae7cufL399eGDRusy4waNUrfffedpk+frkKFCmnjxo1q3bq1smXLpqpVq2rZsmVq2rSpjh49Ki8vL6VJk8a67oIFC9StWzdt2bIlzte/efOmatSoody5c+vnn39Wjhw5FBgYqKioqCT9vgEAjkNwAgCkOq1bt9agQYN05swZSdKWLVu0ePFia3C6d++eRo4cqTVr1qhy5cqSpPz582vz5s2aMWOGatSoocyZM0uSvL29lTFjRpvtFypUSF988cUjX3/RokW6dOmSdu3aZd1OwYIFE/m7BACkJAQnAECqky1bNjVo0EDz58+XMUYNGjRQ1qxZrfNPnDih27dvq06dOjbrhYeH29zO9yh+fn6Pnb93716VK1fOGpoAAM8+ghMAIFXq0KGDevToIUmaMmWKzbybN29Kkn777Tflzp3bZl58BnhIly7dY+fHvK0PAPB8IDgBAFKlevXqKTw8XBaLxTqAQ7TixYvL3d1dwcHBqlGjRpzru7m5SZIiIyMT/NqlS5fW7NmzdfXqVa46AcBzglH1AACpkrOzsw4fPqx//vlHzs7ONvPSp0+vvn37qnfv3lqwYIFOnjypwMBAff3111qwYIEkKW/evLJYLPr111916dIl61Wq+GjZsqVy5MihJk2aaMuWLTp16pSWLVumbdu2Jer3CABIOQhOAIBUy8vLS15eXnHOGzFihD755BONGjVKxYoVU7169fTbb7/J19dXkpQ7d24NGzZMAwcOVPbs2a23/cWHm5ub/vzzT3l7e6t+/foqVaqURo8eHSvAAQCeHRZjjHF0EQAAAACQknHFCQAAAADsIDgBAAAAgB0EJwAAAACwg+AEAAAAAHYQnAAAAADADoITAAAAANhBcAIAAAAAOwhOAAAAAGAHwQkAAAAA7CA4AQAAAIAdBCcAAAAAsIPgBAAAAAB2/D8MKDmLbVkh/QAAAABJRU5ErkJggg==",
      "text/plain": [
       "<Figure size 1000x500 with 1 Axes>"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "plt_df = summary_metrics.sort_values(\"average_value\", ascending=False)\n",
    "\n",
    "import matplotlib.pyplot as plt\n",
    "\n",
    "plt.figure(figsize=(10, 5))\n",
    "plt.bar(plt_df[\"metric\"], plt_df[\"average_value\"])\n",
    "plt.xlabel(\"Metric\")\n",
    "plt.ylabel(\"Average Value\")\n",
    "plt.title(\"Inventory RAG Retrieval Evaluation Metrics\")\n",
    "plt.xticks(rotation=45)\n",
    "plt.ylim(0, 1.05)\n",
    "plt.show()"
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
     "nuid": "c96623f2-5965-40e6-bc10-06f431da5d0b",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>retrieval_score</th><th>final_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>0.1</td><td>0.0966</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>0.1</td><td>0.0965</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>0.1</td><td>0.0964</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>0.1</td><td>0.0964</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>0.1</td><td>0.0964</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         "22752_WH003",
         "22752",
         "SET 7 BABUSHKA NESTING BOXES",
         "WH003",
         "High Risk",
         "Reorder Needed",
         0.914,
         0.1,
         0.0966,
         "Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0."
        ],
        [
         "84406B_WH002",
         "84406B",
         "CREAM CUPID HEARTS COAT HANGER",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.912,
         0.1,
         0.0965,
         "Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25."
        ],
        [
         "84029E_WH002",
         "84029E",
         "RED WOOLLY HOTTIE WHITE HEART",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.909,
         0.1,
         0.0964,
         "Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0."
        ],
        [
         "84029G_WH002",
         "84029G",
         "KNITTED UNION FLAG HOT WATER BOTTLE",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.909,
         0.1,
         0.0964,
         "Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5."
        ],
        [
         "22633_WH003",
         "22633",
         "HAND WARMER UNION JACK",
         "WH003",
         "High Risk",
         "Reorder Needed",
         0.909,
         0.1,
         0.0964,
         "Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0."
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "document_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "stock_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "stockout_risk_level",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "reorder_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "business_priority_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "retrieval_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "final_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "rag_document_text",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "sample_query = \"Which products should be reordered?\"\n",
    "\n",
    "display(\n",
    "    retrieve_inventory_context(sample_query, top_k=5)\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "c9d11f77-ddbe-48e4-b608-c7a727bc5b7d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# RAG Evaluation Summary\n",
    "\n",
    "This notebook evaluates the inventory retrieval layer using a small set of business queries.\n",
    "\n",
    "The evaluation uses term-matching relevance. A retrieved document is considered relevant if it contains the expected business signal, such as `High Risk` or `Reorder Needed`.\n",
    "\n",
    "Metrics calculated:\n",
    "\n",
    "- MRR\n",
    "- Precision@1\n",
    "- Precision@3\n",
    "- Precision@5\n",
    "- Recall@3\n",
    "- Recall@5\n",
    "- NDCG@3\n",
    "- NDCG@5\n",
    "\n",
    "This is a simple v1 evaluation approach. A stronger future version would use LLM-judged relevance and a larger evaluation dataset."
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
   "notebookName": "08_genai_rag_evaluation",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}