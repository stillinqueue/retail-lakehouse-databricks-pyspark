{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "8258ad92-2f4d-490a-8c3f-a09531149bcf",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 07 GenAI Inventory RAG Retrieval\n",
    "\n",
    "This notebook retrieves relevant inventory context from the `inventory_rag_documents` table.\n",
    "\n",
    "The retrieval logic combines simple keyword matching with business priority scoring.\n",
    "\n",
    "This is the first version of the inventory RAG assistant retrieval layer."
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
     "nuid": "0b05bc2e-d80c-4a92-b65a-b6ceea74b00c",
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
     "nuid": "c7807415-b4c1-4cc6-9d1b-e9b3c149e957",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
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
     "nuid": "2885cc5e-be62-45f3-a629-f06093bf3516",
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
     "nuid": "c2e123ca-a731-4f20-9168-98dd092a3de5",
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
     "nuid": "b17d9446-be50-4d4d-8487-fb5befabb8e8",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>retrieval_score</th><th>final_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>1.0</td><td>0.9656</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>1.0</td><td>0.9648</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr></tbody></table></div>"
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
         1.0,
         0.9656,
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
         1.0,
         0.9648,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
    "display(\n",
    "    retrieve_inventory_context(\"Reorder Needed\", top_k=5)\n",
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
     "nuid": "577b6612-1396-430a-a618-2ef8c3cac44c",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>retrieval_score</th><th>final_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>1.0</td><td>0.9656</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>1.0</td><td>0.9648</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr></tbody></table></div>"
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
         1.0,
         0.9656,
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
         1.0,
         0.9648,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
    "display(\n",
    "    retrieve_inventory_context(\"High Risk\", top_k=5)\n",
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
     "nuid": "36965013-b7f9-49b0-877c-ede25a930267",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>retrieval_score</th><th>final_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>1.0</td><td>0.9656</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>1.0</td><td>0.9648</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>1.0</td><td>0.9636</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr></tbody></table></div>"
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
         1.0,
         0.9656,
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
         1.0,
         0.9648,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
         1.0,
         0.9636,
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
    "display(\n",
    "    retrieve_inventory_context(\"lead time\", top_k=5)\n",
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
     "nuid": "6c9bf9a0-b1a3-426a-8fe7-f7173627fac1",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>business_priority_score</th><th>retrieval_score</th><th>final_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>85123A_WH001</td><td>85123A</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>WH001</td><td>High Risk</td><td>No Reorder Needed</td><td>0.606</td><td>1.0</td><td>0.8424</td><td>Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5.</td></tr><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.914</td><td>0.1</td><td>0.0966</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.912</td><td>0.1</td><td>0.0965</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>0.1</td><td>0.0964</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.909</td><td>0.1</td><td>0.0964</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr></tbody></table></div>"
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
         "85123A_WH001",
         "85123A",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "WH001",
         "High Risk",
         "No Reorder Needed",
         0.606,
         1.0,
         0.8424,
         "Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5."
        ],
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
    "display(\n",
    "    retrieve_inventory_context(\"85123A\", top_k=5)\n",
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
     "nuid": "63129697-8032-4c93-a32b-65c331b27a2d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "def generate_simple_inventory_answer(query: str, top_k: int = 3):\n",
    "    results = retrieve_inventory_context(query, top_k=top_k).toPandas()\n",
    "\n",
    "    if results.empty:\n",
    "        return \"The available context does not contain enough information to answer this question reliably.\"\n",
    "\n",
    "    answer_lines = []\n",
    "    answer_lines.append(f\"Question: {query}\")\n",
    "    answer_lines.append(\"\")\n",
    "    answer_lines.append(\"Relevant inventory context:\")\n",
    "\n",
    "    for _, row in results.iterrows():\n",
    "        answer_lines.append(\"\")\n",
    "        answer_lines.append(\n",
    "            f\"- Product {row['stock_code']} ({row['product_name']}) \"\n",
    "            f\"in warehouse {row['warehouse_id']} has stockout risk '{row['stockout_risk_level']}' \"\n",
    "            f\"and reorder status '{row['reorder_flag']}'. \"\n",
    "            f\"Business priority score: {row['business_priority_score']}.\"\n",
    "        )\n",
    "\n",
    "    answer_lines.append(\"\")\n",
    "    answer_lines.append(\"Recommendation:\")\n",
    "    answer_lines.append(\n",
    "        \"Review the highest-priority records first, especially products marked as High Risk or Reorder Needed.\"\n",
    "    )\n",
    "\n",
    "    return \"\\n\".join(answer_lines)"
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
     "nuid": "5b0e220a-31f1-4352-a967-7d062301c094",
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
      "Question: Which products should be reordered?\n\nRelevant inventory context:\n\n- Product 22752 (SET 7 BABUSHKA NESTING BOXES) in warehouse WH003 has stockout risk 'High Risk' and reorder status 'Reorder Needed'. Business priority score: 0.914.\n\n- Product 84406B (CREAM CUPID HEARTS COAT HANGER) in warehouse WH002 has stockout risk 'High Risk' and reorder status 'Reorder Needed'. Business priority score: 0.912.\n\n- Product 84029G (KNITTED UNION FLAG HOT WATER BOTTLE) in warehouse WH002 has stockout risk 'High Risk' and reorder status 'Reorder Needed'. Business priority score: 0.909.\n\nRecommendation:\nReview the highest-priority records first, especially products marked as High Risk or Reorder Needed.\n"
     ]
    }
   ],
   "source": [
    "print(generate_simple_inventory_answer(\"Which products should be reordered?\", top_k=3))"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "16e2ae6d-14ab-4464-905c-e7f6dd0d4c75",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Retrieval Layer Completed\n",
    "\n",
    "This notebook created a simple retrieval layer for the inventory RAG assistant.\n",
    "\n",
    "The retrieval function searches the `inventory_rag_documents` table and ranks records using:\n",
    "\n",
    "- Text relevance\n",
    "- Stockout risk\n",
    "- Reorder status\n",
    "- Supplier reliability\n",
    "- Business priority score\n",
    "\n",
    "The ranking formula is:\n",
    "\n",
    "```text\n",
    "final_score = retrieval_score * (0.6 + 0.4 * business_priority_score)"
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
   "notebookName": "07_genai_inventory_rag_retrieval",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}