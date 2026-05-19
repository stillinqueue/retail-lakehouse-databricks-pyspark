{
 "cells": [
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "e7467bb7-12d2-4365-99dc-b8e855b61c9e",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# 06 GenAI Inventory RAG Documents\n",
    "\n",
    "This notebook creates an inventory RAG document table from Gold inventory KPI tables.\n",
    "\n",
    "The goal is to convert structured inventory records into readable text documents that can be retrieved later by a GenAI assistant."
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
     "nuid": "e79a2690-5f34-40ae-8ae3-14871e717c57",
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
     "nuid": "b313679f-3820-4da9-8915-569a0b41cca2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql.functions import col, concat, lit, coalesce, round, when"
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
     "nuid": "dfeb1bec-1921-4c0c-9ea4-d5f7fbe30bd2",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "inventory_status_df = spark.table(\"workspace.retail_capstone.gold_inventory_status\")\n",
    "sales_velocity_df = spark.table(\"workspace.retail_capstone.gold_product_sales_velocity\")\n",
    "stockout_risk_df = spark.table(\"workspace.retail_capstone.gold_stockout_risk\")\n",
    "reorder_df = spark.table(\"workspace.retail_capstone.gold_reorder_recommendations\")\n",
    "inventory_value_df = spark.table(\"workspace.retail_capstone.gold_inventory_value\")"
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
     "nuid": "7796bc22-36fc-4b86-885c-9e9dbf0dcf85",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>stock_code</th><th>product_name</th><th>category</th><th>brand</th><th>warehouse_id</th><th>warehouse_name</th><th>available_stock</th><th>avg_daily_sales</th><th>days_of_inventory_remaining</th><th>supplier_name</th><th>lead_time_days</th><th>reliability_score</th><th>stockout_risk_level</th></tr></thead><tbody><tr><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>Home & Living</td><td>WarmHome</td><td>WH002</td><td>Berlin Distribution Hub</td><td>25</td><td>17.91</td><td>1.4</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td></tr><tr><td>85123A</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>210</td><td>120.15</td><td>1.75</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td></tr><tr><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>Home & Living</td><td>WarmHome</td><td>WH002</td><td>Berlin Distribution Hub</td><td>48</td><td>38.24</td><td>1.26</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td></tr><tr><td>84879</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>100</td><td>117.5</td><td>0.85</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td></tr><tr><td>71053</td><td>WHITE METAL LANTERN</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>75</td><td>10.41</td><td>7.2</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>Medium Risk</td></tr><tr><td>21730</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>Home Decor</td><td>GlassWorks</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>110</td><td>6.29</td><td>17.49</td><td>GlassWorks Studio</td><td>9</td><td>0.89</td><td>Low Risk</td></tr><tr><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>Gifts</td><td>GiftCraft</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>20</td><td>9.2</td><td>2.17</td><td>GiftCraft Wholesale</td><td>12</td><td>0.86</td><td>High Risk</td></tr><tr><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>Home Decor</td><td>Generic Home</td><td>WH002</td><td>Berlin Distribution Hub</td><td>40</td><td>12.52</td><td>3.19</td><td>DecorCraft Europe</td><td>10</td><td>0.88</td><td>High Risk</td></tr><tr><td>22633</td><td>HAND WARMER UNION JACK</td><td>Accessories</td><td>WarmHome</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>60</td><td>45.52</td><td>1.32</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td></tr><tr><td>22632</td><td>HAND WARMER RED POLKA DOT</td><td>Accessories</td><td>WarmHome</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>50</td><td>43.69</td><td>1.14</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td></tr></tbody></table></div>"
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
         "84029G",
         "KNITTED UNION FLAG HOT WATER BOTTLE",
         "Home & Living",
         "WarmHome",
         "WH002",
         "Berlin Distribution Hub",
         25,
         17.91,
         1.4,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk"
        ],
        [
         "85123A",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         210,
         120.15,
         1.75,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk"
        ],
        [
         "84029E",
         "RED WOOLLY HOTTIE WHITE HEART",
         "Home & Living",
         "WarmHome",
         "WH002",
         "Berlin Distribution Hub",
         48,
         38.24,
         1.26,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk"
        ],
        [
         "84879",
         "ASSORTED COLOUR BIRD ORNAMENT",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         100,
         117.5,
         0.85,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk"
        ],
        [
         "71053",
         "WHITE METAL LANTERN",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         75,
         10.41,
         7.2,
         "Global Home Supplies",
         7,
         0.94,
         "Medium Risk"
        ],
        [
         "21730",
         "GLASS STAR FROSTED T-LIGHT HOLDER",
         "Home Decor",
         "GlassWorks",
         "WH001",
         "Nuremberg Fulfillment Center",
         110,
         6.29,
         17.49,
         "GlassWorks Studio",
         9,
         0.89,
         "Low Risk"
        ],
        [
         "22752",
         "SET 7 BABUSHKA NESTING BOXES",
         "Gifts",
         "GiftCraft",
         "WH003",
         "Cologne Regional Warehouse",
         20,
         9.2,
         2.17,
         "GiftCraft Wholesale",
         12,
         0.86,
         "High Risk"
        ],
        [
         "84406B",
         "CREAM CUPID HEARTS COAT HANGER",
         "Home Decor",
         "Generic Home",
         "WH002",
         "Berlin Distribution Hub",
         40,
         12.52,
         3.19,
         "DecorCraft Europe",
         10,
         0.88,
         "High Risk"
        ],
        [
         "22633",
         "HAND WARMER UNION JACK",
         "Accessories",
         "WarmHome",
         "WH003",
         "Cologne Regional Warehouse",
         60,
         45.52,
         1.32,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk"
        ],
        [
         "22632",
         "HAND WARMER RED POLKA DOT",
         "Accessories",
         "WarmHome",
         "WH003",
         "Cologne Regional Warehouse",
         50,
         43.69,
         1.14,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk"
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
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "brand",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "available_stock",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "avg_daily_sales",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "days_of_inventory_remaining",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "supplier_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "lead_time_days",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reliability_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "stockout_risk_level",
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
    "display(stockout_risk_df)"
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
     "nuid": "1774f053-3fdc-45f1-b608-ceee5e163aaa",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "rag_base_df = stockout_risk_df.join(\n",
    "    reorder_df.select(\n",
    "        \"stock_code\",\n",
    "        \"warehouse_id\",\n",
    "        \"reorder_level\",\n",
    "        \"reorder_quantity\",\n",
    "        \"recommended_reorder_quantity\",\n",
    "        \"reorder_flag\"\n",
    "    ),\n",
    "    on=[\"stock_code\", \"warehouse_id\"],\n",
    "    how=\"left\"\n",
    ").join(\n",
    "    inventory_value_df.select(\n",
    "        \"stock_code\",\n",
    "        \"warehouse_id\",\n",
    "        \"current_stock\",\n",
    "        \"unit_cost\",\n",
    "        \"inventory_value\"\n",
    "    ),\n",
    "    on=[\"stock_code\", \"warehouse_id\"],\n",
    "    how=\"left\"\n",
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
     "nuid": "30adc315-ebaa-4d26-a07d-c51f602bd407",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>stock_code</th><th>warehouse_id</th><th>product_name</th><th>category</th><th>brand</th><th>warehouse_name</th><th>available_stock</th><th>avg_daily_sales</th><th>days_of_inventory_remaining</th><th>supplier_name</th><th>lead_time_days</th><th>reliability_score</th><th>stockout_risk_level</th><th>reorder_level</th><th>reorder_quantity</th><th>recommended_reorder_quantity</th><th>reorder_flag</th><th>current_stock</th><th>unit_cost</th><th>inventory_value</th></tr></thead><tbody><tr><td>84029G</td><td>WH002</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>Home & Living</td><td>WarmHome</td><td>Berlin Distribution Hub</td><td>25</td><td>17.91</td><td>1.4</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>50</td><td>120</td><td>120</td><td>Reorder Needed</td><td>35</td><td>3.5</td><td>122.5</td></tr><tr><td>85123A</td><td>WH001</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>Home Decor</td><td>Generic Home</td><td>Nuremberg Fulfillment Center</td><td>210</td><td>120.15</td><td>1.75</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td><td>100</td><td>300</td><td>0</td><td>No Reorder Needed</td><td>250</td><td>1.25</td><td>312.5</td></tr><tr><td>84029E</td><td>WH002</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>Home & Living</td><td>WarmHome</td><td>Berlin Distribution Hub</td><td>48</td><td>38.24</td><td>1.26</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>50</td><td>120</td><td>120</td><td>Reorder Needed</td><td>60</td><td>3.25</td><td>195.0</td></tr><tr><td>84879</td><td>WH001</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>Home Decor</td><td>Generic Home</td><td>Nuremberg Fulfillment Center</td><td>100</td><td>117.5</td><td>0.85</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td><td>90</td><td>250</td><td>0</td><td>No Reorder Needed</td><td>110</td><td>1.5</td><td>165.0</td></tr><tr><td>71053</td><td>WH001</td><td>WHITE METAL LANTERN</td><td>Home Decor</td><td>Generic Home</td><td>Nuremberg Fulfillment Center</td><td>75</td><td>10.41</td><td>7.2</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>Medium Risk</td><td>80</td><td>200</td><td>200</td><td>Reorder Needed</td><td>90</td><td>2.1</td><td>189.0</td></tr><tr><td>21730</td><td>WH001</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>Home Decor</td><td>GlassWorks</td><td>Nuremberg Fulfillment Center</td><td>110</td><td>6.29</td><td>17.49</td><td>GlassWorks Studio</td><td>9</td><td>0.89</td><td>Low Risk</td><td>70</td><td>180</td><td>0</td><td>No Reorder Needed</td><td>140</td><td>2.75</td><td>385.0</td></tr><tr><td>22752</td><td>WH003</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>Gifts</td><td>GiftCraft</td><td>Cologne Regional Warehouse</td><td>20</td><td>9.2</td><td>2.17</td><td>GiftCraft Wholesale</td><td>12</td><td>0.86</td><td>High Risk</td><td>40</td><td>100</td><td>100</td><td>Reorder Needed</td><td>25</td><td>4.0</td><td>100.0</td></tr><tr><td>84406B</td><td>WH002</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>Home Decor</td><td>Generic Home</td><td>Berlin Distribution Hub</td><td>40</td><td>12.52</td><td>3.19</td><td>DecorCraft Europe</td><td>10</td><td>0.88</td><td>High Risk</td><td>60</td><td>150</td><td>150</td><td>Reorder Needed</td><td>45</td><td>1.85</td><td>83.25</td></tr><tr><td>22633</td><td>WH003</td><td>HAND WARMER UNION JACK</td><td>Accessories</td><td>WarmHome</td><td>Cologne Regional Warehouse</td><td>60</td><td>45.52</td><td>1.32</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>120</td><td>300</td><td>300</td><td>Reorder Needed</td><td>80</td><td>1.1</td><td>88.0</td></tr><tr><td>22632</td><td>WH003</td><td>HAND WARMER RED POLKA DOT</td><td>Accessories</td><td>WarmHome</td><td>Cologne Regional Warehouse</td><td>50</td><td>43.69</td><td>1.14</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>120</td><td>300</td><td>300</td><td>Reorder Needed</td><td>75</td><td>1.1</td><td>82.5</td></tr></tbody></table></div>"
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
         "84029G",
         "WH002",
         "KNITTED UNION FLAG HOT WATER BOTTLE",
         "Home & Living",
         "WarmHome",
         "Berlin Distribution Hub",
         25,
         17.91,
         1.4,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         50,
         120,
         120,
         "Reorder Needed",
         35,
         3.5,
         122.5
        ],
        [
         "85123A",
         "WH001",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "Home Decor",
         "Generic Home",
         "Nuremberg Fulfillment Center",
         210,
         120.15,
         1.75,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk",
         100,
         300,
         0,
         "No Reorder Needed",
         250,
         1.25,
         312.5
        ],
        [
         "84029E",
         "WH002",
         "RED WOOLLY HOTTIE WHITE HEART",
         "Home & Living",
         "WarmHome",
         "Berlin Distribution Hub",
         48,
         38.24,
         1.26,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         50,
         120,
         120,
         "Reorder Needed",
         60,
         3.25,
         195.0
        ],
        [
         "84879",
         "WH001",
         "ASSORTED COLOUR BIRD ORNAMENT",
         "Home Decor",
         "Generic Home",
         "Nuremberg Fulfillment Center",
         100,
         117.5,
         0.85,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk",
         90,
         250,
         0,
         "No Reorder Needed",
         110,
         1.5,
         165.0
        ],
        [
         "71053",
         "WH001",
         "WHITE METAL LANTERN",
         "Home Decor",
         "Generic Home",
         "Nuremberg Fulfillment Center",
         75,
         10.41,
         7.2,
         "Global Home Supplies",
         7,
         0.94,
         "Medium Risk",
         80,
         200,
         200,
         "Reorder Needed",
         90,
         2.1,
         189.0
        ],
        [
         "21730",
         "WH001",
         "GLASS STAR FROSTED T-LIGHT HOLDER",
         "Home Decor",
         "GlassWorks",
         "Nuremberg Fulfillment Center",
         110,
         6.29,
         17.49,
         "GlassWorks Studio",
         9,
         0.89,
         "Low Risk",
         70,
         180,
         0,
         "No Reorder Needed",
         140,
         2.75,
         385.0
        ],
        [
         "22752",
         "WH003",
         "SET 7 BABUSHKA NESTING BOXES",
         "Gifts",
         "GiftCraft",
         "Cologne Regional Warehouse",
         20,
         9.2,
         2.17,
         "GiftCraft Wholesale",
         12,
         0.86,
         "High Risk",
         40,
         100,
         100,
         "Reorder Needed",
         25,
         4.0,
         100.0
        ],
        [
         "84406B",
         "WH002",
         "CREAM CUPID HEARTS COAT HANGER",
         "Home Decor",
         "Generic Home",
         "Berlin Distribution Hub",
         40,
         12.52,
         3.19,
         "DecorCraft Europe",
         10,
         0.88,
         "High Risk",
         60,
         150,
         150,
         "Reorder Needed",
         45,
         1.85,
         83.25
        ],
        [
         "22633",
         "WH003",
         "HAND WARMER UNION JACK",
         "Accessories",
         "WarmHome",
         "Cologne Regional Warehouse",
         60,
         45.52,
         1.32,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         120,
         300,
         300,
         "Reorder Needed",
         80,
         1.1,
         88.0
        ],
        [
         "22632",
         "WH003",
         "HAND WARMER RED POLKA DOT",
         "Accessories",
         "WarmHome",
         "Cologne Regional Warehouse",
         50,
         43.69,
         1.14,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         120,
         300,
         300,
         "Reorder Needed",
         75,
         1.1,
         82.5
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
         "name": "stock_code",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "product_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "brand",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "available_stock",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "avg_daily_sales",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "days_of_inventory_remaining",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "supplier_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "lead_time_days",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reliability_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "stockout_risk_level",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "reorder_level",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reorder_quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "recommended_reorder_quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reorder_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "current_stock",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "unit_cost",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "inventory_value",
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
    "display(rag_base_df)"
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
     "nuid": "f1b4eba9-8351-4fca-98cd-bc44d07a6e22",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "rag_scored_df = rag_base_df.withColumn(\n",
    "    \"risk_score\",\n",
    "    when(col(\"stockout_risk_level\") == \"High Risk\", 1.0)\n",
    "    .when(col(\"stockout_risk_level\") == \"Medium Risk\", 0.75)\n",
    "    .when(col(\"stockout_risk_level\") == \"Low Risk\", 0.40)\n",
    "    .when(col(\"stockout_risk_level\") == \"No Recent Sales\", 0.20)\n",
    "    .otherwise(0.10)\n",
    ").withColumn(\n",
    "    \"reorder_score\",\n",
    "    when(col(\"reorder_flag\") == \"Reorder Needed\", 1.0)\n",
    "    .otherwise(0.0)\n",
    ").withColumn(\n",
    "    \"supplier_risk_score\",\n",
    "    1 - col(\"reliability_score\")\n",
    ").withColumn(\n",
    "    \"business_priority_score\",\n",
    "    round(\n",
    "        (col(\"risk_score\") * 0.6) +\n",
    "        (col(\"reorder_score\") * 0.3) +\n",
    "        (col(\"supplier_risk_score\") * 0.1),\n",
    "        3\n",
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
     "nuid": "df5f0531-e41b-42e9-9efd-1c9826c5129f",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>stock_code</th><th>product_name</th><th>warehouse_id</th><th>stockout_risk_level</th><th>reorder_flag</th><th>reliability_score</th><th>business_priority_score</th></tr></thead><tbody><tr><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.91</td><td>0.909</td></tr><tr><td>85123A</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>WH001</td><td>High Risk</td><td>No Reorder Needed</td><td>0.94</td><td>0.606</td></tr><tr><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.91</td><td>0.909</td></tr><tr><td>84879</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>WH001</td><td>High Risk</td><td>No Reorder Needed</td><td>0.94</td><td>0.606</td></tr><tr><td>71053</td><td>WHITE METAL LANTERN</td><td>WH001</td><td>Medium Risk</td><td>Reorder Needed</td><td>0.94</td><td>0.756</td></tr><tr><td>21730</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>WH001</td><td>Low Risk</td><td>No Reorder Needed</td><td>0.89</td><td>0.251</td></tr><tr><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.86</td><td>0.914</td></tr><tr><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>WH002</td><td>High Risk</td><td>Reorder Needed</td><td>0.88</td><td>0.912</td></tr><tr><td>22633</td><td>HAND WARMER UNION JACK</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.91</td><td>0.909</td></tr><tr><td>22632</td><td>HAND WARMER RED POLKA DOT</td><td>WH003</td><td>High Risk</td><td>Reorder Needed</td><td>0.91</td><td>0.909</td></tr></tbody></table></div>"
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
         "84029G",
         "KNITTED UNION FLAG HOT WATER BOTTLE",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.91,
         0.909
        ],
        [
         "85123A",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "WH001",
         "High Risk",
         "No Reorder Needed",
         0.94,
         0.606
        ],
        [
         "84029E",
         "RED WOOLLY HOTTIE WHITE HEART",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.91,
         0.909
        ],
        [
         "84879",
         "ASSORTED COLOUR BIRD ORNAMENT",
         "WH001",
         "High Risk",
         "No Reorder Needed",
         0.94,
         0.606
        ],
        [
         "71053",
         "WHITE METAL LANTERN",
         "WH001",
         "Medium Risk",
         "Reorder Needed",
         0.94,
         0.756
        ],
        [
         "21730",
         "GLASS STAR FROSTED T-LIGHT HOLDER",
         "WH001",
         "Low Risk",
         "No Reorder Needed",
         0.89,
         0.251
        ],
        [
         "22752",
         "SET 7 BABUSHKA NESTING BOXES",
         "WH003",
         "High Risk",
         "Reorder Needed",
         0.86,
         0.914
        ],
        [
         "84406B",
         "CREAM CUPID HEARTS COAT HANGER",
         "WH002",
         "High Risk",
         "Reorder Needed",
         0.88,
         0.912
        ],
        [
         "22633",
         "HAND WARMER UNION JACK",
         "WH003",
         "High Risk",
         "Reorder Needed",
         0.91,
         0.909
        ],
        [
         "22632",
         "HAND WARMER RED POLKA DOT",
         "WH003",
         "High Risk",
         "Reorder Needed",
         0.91,
         0.909
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
         "name": "reliability_score",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "business_priority_score",
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
    "display(\n",
    "    rag_scored_df.select(\n",
    "        \"stock_code\",\n",
    "        \"product_name\",\n",
    "        \"warehouse_id\",\n",
    "        \"stockout_risk_level\",\n",
    "        \"reorder_flag\",\n",
    "        \"reliability_score\",\n",
    "        \"business_priority_score\"\n",
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
     "nuid": "3e379df9-fed3-4245-ac9b-3efd6d910f15",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "rag_documents_df = rag_scored_df.withColumn(\n",
    "    \"document_id\",\n",
    "    concat(\n",
    "        col(\"stock_code\"),\n",
    "        lit(\"_\"),\n",
    "        col(\"warehouse_id\")\n",
    "    )\n",
    ").withColumn(\n",
    "    \"rag_document_text\",\n",
    "    concat(\n",
    "        lit(\"Product \"),\n",
    "        col(\"stock_code\"),\n",
    "        lit(\", \"),\n",
    "        col(\"product_name\"),\n",
    "        lit(\", belongs to category \"),\n",
    "        col(\"category\"),\n",
    "        lit(\" and brand \"),\n",
    "        col(\"brand\"),\n",
    "        lit(\". It is stored in warehouse \"),\n",
    "        col(\"warehouse_id\"),\n",
    "        lit(\" named \"),\n",
    "        col(\"warehouse_name\"),\n",
    "        lit(\". Available stock is \"),\n",
    "        col(\"available_stock\").cast(\"string\"),\n",
    "        lit(\" units. Current stock is \"),\n",
    "        col(\"current_stock\").cast(\"string\"),\n",
    "        lit(\" units. Average daily sales is \"),\n",
    "        col(\"avg_daily_sales\").cast(\"string\"),\n",
    "        lit(\" units. Days of inventory remaining is \"),\n",
    "        col(\"days_of_inventory_remaining\").cast(\"string\"),\n",
    "        lit(\". Supplier is \"),\n",
    "        col(\"supplier_name\"),\n",
    "        lit(\" with lead time of \"),\n",
    "        col(\"lead_time_days\").cast(\"string\"),\n",
    "        lit(\" days and reliability score of \"),\n",
    "        col(\"reliability_score\").cast(\"string\"),\n",
    "        lit(\". Stockout risk level is \"),\n",
    "        col(\"stockout_risk_level\"),\n",
    "        lit(\". Reorder status is \"),\n",
    "        col(\"reorder_flag\"),\n",
    "        lit(\". Recommended reorder quantity is \"),\n",
    "        col(\"recommended_reorder_quantity\").cast(\"string\"),\n",
    "        lit(\" units. Inventory value is \"),\n",
    "        col(\"inventory_value\").cast(\"string\"),\n",
    "        lit(\".\")\n",
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
     "nuid": "b5bddea2-b7e3-4859-a756-35f5b9787531",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "inventory_rag_documents_df = rag_documents_df.select(\n",
    "    \"document_id\",\n",
    "    \"stock_code\",\n",
    "    \"product_name\",\n",
    "    \"category\",\n",
    "    \"brand\",\n",
    "    \"warehouse_id\",\n",
    "    \"warehouse_name\",\n",
    "    \"available_stock\",\n",
    "    \"avg_daily_sales\",\n",
    "    \"days_of_inventory_remaining\",\n",
    "    \"supplier_name\",\n",
    "    \"lead_time_days\",\n",
    "    \"reliability_score\",\n",
    "    \"stockout_risk_level\",\n",
    "    \"reorder_flag\",\n",
    "    \"recommended_reorder_quantity\",\n",
    "    \"inventory_value\",\n",
    "    \"business_priority_score\",\n",
    "    \"rag_document_text\"\n",
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
     "nuid": "ce044d79-bf05-406a-9d2c-21940ad4e631",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>stock_code</th><th>product_name</th><th>category</th><th>brand</th><th>warehouse_id</th><th>warehouse_name</th><th>available_stock</th><th>avg_daily_sales</th><th>days_of_inventory_remaining</th><th>supplier_name</th><th>lead_time_days</th><th>reliability_score</th><th>stockout_risk_level</th><th>reorder_flag</th><th>recommended_reorder_quantity</th><th>inventory_value</th><th>business_priority_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>84029G_WH002</td><td>84029G</td><td>KNITTED UNION FLAG HOT WATER BOTTLE</td><td>Home & Living</td><td>WarmHome</td><td>WH002</td><td>Berlin Distribution Hub</td><td>25</td><td>17.91</td><td>1.4</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>Reorder Needed</td><td>120</td><td>122.5</td><td>0.909</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>85123A_WH001</td><td>85123A</td><td>WHITE HANGING HEART T-LIGHT HOLDER</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>210</td><td>120.15</td><td>1.75</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td><td>No Reorder Needed</td><td>0</td><td>312.5</td><td>0.606</td><td>Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5.</td></tr><tr><td>84029E_WH002</td><td>84029E</td><td>RED WOOLLY HOTTIE WHITE HEART</td><td>Home & Living</td><td>WarmHome</td><td>WH002</td><td>Berlin Distribution Hub</td><td>48</td><td>38.24</td><td>1.26</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>Reorder Needed</td><td>120</td><td>195.0</td><td>0.909</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84879_WH001</td><td>84879</td><td>ASSORTED COLOUR BIRD ORNAMENT</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>100</td><td>117.5</td><td>0.85</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>High Risk</td><td>No Reorder Needed</td><td>0</td><td>165.0</td><td>0.606</td><td>Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0.</td></tr><tr><td>71053_WH001</td><td>71053</td><td>WHITE METAL LANTERN</td><td>Home Decor</td><td>Generic Home</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>75</td><td>10.41</td><td>7.2</td><td>Global Home Supplies</td><td>7</td><td>0.94</td><td>Medium Risk</td><td>Reorder Needed</td><td>200</td><td>189.0</td><td>0.756</td><td>Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0.</td></tr><tr><td>21730_WH001</td><td>21730</td><td>GLASS STAR FROSTED T-LIGHT HOLDER</td><td>Home Decor</td><td>GlassWorks</td><td>WH001</td><td>Nuremberg Fulfillment Center</td><td>110</td><td>6.29</td><td>17.49</td><td>GlassWorks Studio</td><td>9</td><td>0.89</td><td>Low Risk</td><td>No Reorder Needed</td><td>0</td><td>385.0</td><td>0.251</td><td>Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0.</td></tr><tr><td>22752_WH003</td><td>22752</td><td>SET 7 BABUSHKA NESTING BOXES</td><td>Gifts</td><td>GiftCraft</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>20</td><td>9.2</td><td>2.17</td><td>GiftCraft Wholesale</td><td>12</td><td>0.86</td><td>High Risk</td><td>Reorder Needed</td><td>100</td><td>100.0</td><td>0.914</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>84406B</td><td>CREAM CUPID HEARTS COAT HANGER</td><td>Home Decor</td><td>Generic Home</td><td>WH002</td><td>Berlin Distribution Hub</td><td>40</td><td>12.52</td><td>3.19</td><td>DecorCraft Europe</td><td>10</td><td>0.88</td><td>High Risk</td><td>Reorder Needed</td><td>150</td><td>83.25</td><td>0.912</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>22633_WH003</td><td>22633</td><td>HAND WARMER UNION JACK</td><td>Accessories</td><td>WarmHome</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>60</td><td>45.52</td><td>1.32</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>Reorder Needed</td><td>300</td><td>88.0</td><td>0.909</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr><tr><td>22632_WH003</td><td>22632</td><td>HAND WARMER RED POLKA DOT</td><td>Accessories</td><td>WarmHome</td><td>WH003</td><td>Cologne Regional Warehouse</td><td>50</td><td>43.69</td><td>1.14</td><td>WarmHome Manufacturing</td><td>14</td><td>0.91</td><td>High Risk</td><td>Reorder Needed</td><td>300</td><td>82.5</td><td>0.909</td><td>Product 22632, HAND WARMER RED POLKA DOT, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 50 units. Current stock is 75 units. Average daily sales is 43.69 units. Days of inventory remaining is 1.14. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 82.5.</td></tr></tbody></table></div>"
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
         "Home & Living",
         "WarmHome",
         "WH002",
         "Berlin Distribution Hub",
         25,
         17.91,
         1.4,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         "Reorder Needed",
         120,
         122.5,
         0.909,
         "Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5."
        ],
        [
         "85123A_WH001",
         "85123A",
         "WHITE HANGING HEART T-LIGHT HOLDER",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         210,
         120.15,
         1.75,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk",
         "No Reorder Needed",
         0,
         312.5,
         0.606,
         "Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5."
        ],
        [
         "84029E_WH002",
         "84029E",
         "RED WOOLLY HOTTIE WHITE HEART",
         "Home & Living",
         "WarmHome",
         "WH002",
         "Berlin Distribution Hub",
         48,
         38.24,
         1.26,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         "Reorder Needed",
         120,
         195.0,
         0.909,
         "Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0."
        ],
        [
         "84879_WH001",
         "84879",
         "ASSORTED COLOUR BIRD ORNAMENT",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         100,
         117.5,
         0.85,
         "Global Home Supplies",
         7,
         0.94,
         "High Risk",
         "No Reorder Needed",
         0,
         165.0,
         0.606,
         "Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0."
        ],
        [
         "71053_WH001",
         "71053",
         "WHITE METAL LANTERN",
         "Home Decor",
         "Generic Home",
         "WH001",
         "Nuremberg Fulfillment Center",
         75,
         10.41,
         7.2,
         "Global Home Supplies",
         7,
         0.94,
         "Medium Risk",
         "Reorder Needed",
         200,
         189.0,
         0.756,
         "Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0."
        ],
        [
         "21730_WH001",
         "21730",
         "GLASS STAR FROSTED T-LIGHT HOLDER",
         "Home Decor",
         "GlassWorks",
         "WH001",
         "Nuremberg Fulfillment Center",
         110,
         6.29,
         17.49,
         "GlassWorks Studio",
         9,
         0.89,
         "Low Risk",
         "No Reorder Needed",
         0,
         385.0,
         0.251,
         "Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0."
        ],
        [
         "22752_WH003",
         "22752",
         "SET 7 BABUSHKA NESTING BOXES",
         "Gifts",
         "GiftCraft",
         "WH003",
         "Cologne Regional Warehouse",
         20,
         9.2,
         2.17,
         "GiftCraft Wholesale",
         12,
         0.86,
         "High Risk",
         "Reorder Needed",
         100,
         100.0,
         0.914,
         "Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0."
        ],
        [
         "84406B_WH002",
         "84406B",
         "CREAM CUPID HEARTS COAT HANGER",
         "Home Decor",
         "Generic Home",
         "WH002",
         "Berlin Distribution Hub",
         40,
         12.52,
         3.19,
         "DecorCraft Europe",
         10,
         0.88,
         "High Risk",
         "Reorder Needed",
         150,
         83.25,
         0.912,
         "Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25."
        ],
        [
         "22633_WH003",
         "22633",
         "HAND WARMER UNION JACK",
         "Accessories",
         "WarmHome",
         "WH003",
         "Cologne Regional Warehouse",
         60,
         45.52,
         1.32,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         "Reorder Needed",
         300,
         88.0,
         0.909,
         "Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0."
        ],
        [
         "22632_WH003",
         "22632",
         "HAND WARMER RED POLKA DOT",
         "Accessories",
         "WarmHome",
         "WH003",
         "Cologne Regional Warehouse",
         50,
         43.69,
         1.14,
         "WarmHome Manufacturing",
         14,
         0.91,
         "High Risk",
         "Reorder Needed",
         300,
         82.5,
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
         "name": "category",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "brand",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "warehouse_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "available_stock",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "avg_daily_sales",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "days_of_inventory_remaining",
         "type": "\"double\""
        },
        {
         "metadata": "{}",
         "name": "supplier_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "lead_time_days",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "reliability_score",
         "type": "\"double\""
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
         "name": "recommended_reorder_quantity",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "inventory_value",
         "type": "\"double\""
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
    "display(inventory_rag_documents_df)"
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
     "nuid": "0fa686c7-85be-4f84-b465-eada10d64356",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "inventory_rag_documents_df.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .option(\"overwriteSchema\", \"true\") \\\n",
    "    .saveAsTable(\"workspace.retail_capstone.inventory_rag_documents\")"
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
     "nuid": "3792b14e-acb0-4c5b-816e-05536b9b75de",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_count</th></tr></thead><tbody><tr><td>10</td></tr></tbody></table></div>"
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
         10
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
         "name": "document_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "spark.sql(\"\"\"\n",
    "SELECT COUNT(*) AS document_count\n",
    "FROM workspace.retail_capstone.inventory_rag_documents\n",
    "\"\"\").display()"
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
     "nuid": "050d85be-3138-4507-803f-c676367e9cde",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>document_id</th><th>business_priority_score</th><th>rag_document_text</th></tr></thead><tbody><tr><td>84029G_WH002</td><td>0.909</td><td>Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5.</td></tr><tr><td>85123A_WH001</td><td>0.606</td><td>Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5.</td></tr><tr><td>84029E_WH002</td><td>0.909</td><td>Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0.</td></tr><tr><td>84879_WH001</td><td>0.606</td><td>Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0.</td></tr><tr><td>71053_WH001</td><td>0.756</td><td>Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0.</td></tr><tr><td>21730_WH001</td><td>0.251</td><td>Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0.</td></tr><tr><td>22752_WH003</td><td>0.914</td><td>Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0.</td></tr><tr><td>84406B_WH002</td><td>0.912</td><td>Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25.</td></tr><tr><td>22633_WH003</td><td>0.909</td><td>Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0.</td></tr><tr><td>22632_WH003</td><td>0.909</td><td>Product 22632, HAND WARMER RED POLKA DOT, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 50 units. Current stock is 75 units. Average daily sales is 43.69 units. Days of inventory remaining is 1.14. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 82.5.</td></tr></tbody></table></div>"
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
         0.909,
         "Product 84029G, KNITTED UNION FLAG HOT WATER BOTTLE, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 25 units. Current stock is 35 units. Average daily sales is 17.91 units. Days of inventory remaining is 1.4. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 122.5."
        ],
        [
         "85123A_WH001",
         0.606,
         "Product 85123A, WHITE HANGING HEART T-LIGHT HOLDER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 210 units. Current stock is 250 units. Average daily sales is 120.15 units. Days of inventory remaining is 1.75. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 312.5."
        ],
        [
         "84029E_WH002",
         0.909,
         "Product 84029E, RED WOOLLY HOTTIE WHITE HEART, belongs to category Home & Living and brand WarmHome. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 48 units. Current stock is 60 units. Average daily sales is 38.24 units. Days of inventory remaining is 1.26. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 120 units. Inventory value is 195.0."
        ],
        [
         "84879_WH001",
         0.606,
         "Product 84879, ASSORTED COLOUR BIRD ORNAMENT, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 100 units. Current stock is 110 units. Average daily sales is 117.5 units. Days of inventory remaining is 0.85. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is High Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 165.0."
        ],
        [
         "71053_WH001",
         0.756,
         "Product 71053, WHITE METAL LANTERN, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 75 units. Current stock is 90 units. Average daily sales is 10.41 units. Days of inventory remaining is 7.2. Supplier is Global Home Supplies with lead time of 7 days and reliability score of 0.94. Stockout risk level is Medium Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 200 units. Inventory value is 189.0."
        ],
        [
         "21730_WH001",
         0.251,
         "Product 21730, GLASS STAR FROSTED T-LIGHT HOLDER, belongs to category Home Decor and brand GlassWorks. It is stored in warehouse WH001 named Nuremberg Fulfillment Center. Available stock is 110 units. Current stock is 140 units. Average daily sales is 6.29 units. Days of inventory remaining is 17.49. Supplier is GlassWorks Studio with lead time of 9 days and reliability score of 0.89. Stockout risk level is Low Risk. Reorder status is No Reorder Needed. Recommended reorder quantity is 0 units. Inventory value is 385.0."
        ],
        [
         "22752_WH003",
         0.914,
         "Product 22752, SET 7 BABUSHKA NESTING BOXES, belongs to category Gifts and brand GiftCraft. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 20 units. Current stock is 25 units. Average daily sales is 9.2 units. Days of inventory remaining is 2.17. Supplier is GiftCraft Wholesale with lead time of 12 days and reliability score of 0.86. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 100 units. Inventory value is 100.0."
        ],
        [
         "84406B_WH002",
         0.912,
         "Product 84406B, CREAM CUPID HEARTS COAT HANGER, belongs to category Home Decor and brand Generic Home. It is stored in warehouse WH002 named Berlin Distribution Hub. Available stock is 40 units. Current stock is 45 units. Average daily sales is 12.52 units. Days of inventory remaining is 3.19. Supplier is DecorCraft Europe with lead time of 10 days and reliability score of 0.88. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 150 units. Inventory value is 83.25."
        ],
        [
         "22633_WH003",
         0.909,
         "Product 22633, HAND WARMER UNION JACK, belongs to category Accessories and brand WarmHome. It is stored in warehouse WH003 named Cologne Regional Warehouse. Available stock is 60 units. Current stock is 80 units. Average daily sales is 45.52 units. Days of inventory remaining is 1.32. Supplier is WarmHome Manufacturing with lead time of 14 days and reliability score of 0.91. Stockout risk level is High Risk. Reorder status is Reorder Needed. Recommended reorder quantity is 300 units. Inventory value is 88.0."
        ],
        [
         "22632_WH003",
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
    "display(\n",
    "    spark.table(\"workspace.retail_capstone.inventory_rag_documents\")\n",
    "    .select(\"document_id\", \"business_priority_score\", \"rag_document_text\")\n",
    ")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "95e4bdce-9c48-42eb-a4ac-d4f2bc19579d",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "source": [
    "# Inventory RAG Document Table Completed\n",
    "\n",
    "This notebook created the `inventory_rag_documents` Delta table.\n",
    "\n",
    "Each row is a natural language document created from Gold inventory KPI tables.\n",
    "\n",
    "The table includes:\n",
    "\n",
    "- Product details\n",
    "- Warehouse details\n",
    "- Available stock\n",
    "- Sales velocity\n",
    "- Days of inventory remaining\n",
    "- Supplier lead time\n",
    "- Stockout risk level\n",
    "- Reorder flag\n",
    "- Recommended reorder quantity\n",
    "- Inventory value\n",
    "- Business priority score\n",
    "\n",
    "The business priority score is used later to rank high-risk inventory records more strongly during retrieval."
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
   "notebookName": "06_genai_inventory_rag_documents",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}