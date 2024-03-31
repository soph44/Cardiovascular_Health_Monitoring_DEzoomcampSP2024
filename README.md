# Project, DE ZoomCamp 2024: U.S. Population Cardiovascular Health Analysis by Location, Category (Age, Sex, Education, Income), & Time

**NOTE!!!, this project has not been built yet during this First Attempt. Below is the outline and architecture I have planned for the project and would still like feedback.**

This is my capstone project for the [DataTalks.Club](https://datatalks.club) [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 2024 Cohort.
 
## Problem Statement
It's a common to think that the health of the general U.S. population has been in decline over the past two decades. Common health news such as the double digit increase in adult obesity rates would make one thing that cardiovascular health is also on the decline.
In actuality, there has been a slight decline in the rate of deaths due to cardiovascular disease (CVD). In 2009, there were 182.8 deaths/100,000 due to heart disease. In 2019, this came down to 161.5 deaths/100,000.

This project aims to explore the data and patterns behind the population's cardiovascular health changes over the past few years and to update itself on a yearly basis for continual monitoring. This will answer several questions:
* Are there specific locations (states) that are more heavily impacted by CVD? And where there has been an increase or decrease in CVD rates?
* Has there been a change in the population age where CVD is prevalent?
* Has there been a change in CVD rates between male and female populations?
* Has CVD rates changed on a socioeconomic group basis (Income, Education, or Ethnicity)?

### Dataset
 The data comes from the CDC's available data on 'Behavorial Risk Factors.' This data is from a continuous, state-based surveillance system for chronic diseases inclusive of cardiovascular health.
 https://data.cdc.gov/Behavioral-Risk-Factors/BRFSS-Graph-of-Current-Prevalence-of-Cardiovascula/gfhd-2f5y

 Collection and quality assurance methodology can be reviewed here: https://github.com/DataTalksClub/data-engineering-zoomcamp

 This dataset can be downloaded on a yearly basis into a .csv file.

## Data Pipeline Architecture

### Technology Stack
* Infrastructure as Code (IaC): Terraform
* Cloud Platform: Google Compute Engine (Virtual Machine)
* Workflow Orchestration: Mage
* Containerization: Docker
* Data Lake: Google Cloud Storage (GCS)
* Data Warehosue: Google BigQuery
* Batch Processing: Python, Pandas, Spark
* Transformations: Spark
* Visualization: Google Looker Studio

### Orchestration
A VM will run a Docker container including Mage and the python scripts described below. Mage will schedule the python script to run on the last day of the year to pull new CDC data and perform the below steps.

### Data Ingestion to Google Cloud Storage
The ingestion_to_gcs.py script will kick off to download yearly data into the local VM as a .csv file and then amended to give a unique identifier to each row. This is because each row in this dataset corresponds to a survey of a specific state and specific population grouping for each survey question. So the unique identifier will be of the format: [State]-[SurveyQuestion]-[SurveyAnswer]-[GroupCategory]-[GroupAnswer].
For example:[CA]-[Question1]-[Yes]-[Age]-[18-34].

Additinoally, there is a 'GeoLocation' column in the format of (Longitude, Latitude) that is a string. The script also parses this into two columns for 'Longitude' and 'Latitude' with double value types.

After this .csv has been amended, it is converted to parquet with an applied schema and then uploaded to a Google Cloud Storage bucket.

### Data Cleaned, Partitioned, Clustered, and Loaded to BigQuery
The process_to_bigquery.py script uses the bigquery library to perform the following actions and create new tables from the bucket data:

Step 1. Unnecessary columns are removed. And any rows with low confidence values are removed to maintain data accuracy. The corresponds to Confidence_limit_Low < 90%.

These removed columns are replicated data columns in an ID format in addition to the text:
- Locationabbr
- Locationdesc
- Class
- Topic
- Question
- Response
- Break_Out
- Break_Out_Category
These removed columns are unused in the downstream queries:
- Display_order
- Data_value_unit
- Data_value_type
- Data_Value_Footnote_Symbol
- Data_Value_Footnote
- DataSource

Step 2. The script partitions the data based on the Survey Question because data each type of Survey Question would not be queried together downstream:
* Question1: "Ever told you had a heart attack (myocardial infarction)?"
* Question 2: "Ever told you had a stroke?"
* Question 3: "Ever told you had angina or coronary heart disease?"
* Question 4: "Respondents that have ever reported having coronary heart disease (chd) or myocardial infarction (mi) (variable calculated from one or more BRFSS questions)"

Step 3. The script then clusters each table by GroupCategory:
* Age Group
* Education Attained
* Gender
* Household Income
* Race/Ethnicity
* Overall (encompassess all groups)

Step 4. Tables are saved into BigQuery.

### Transformations via Spark
The transform_in_bigquery.py script uses Spark to outer join each yearly table for each survey question. The Spark server will run in the VM instance.

### Dashboard
A dashboard is made for each Survey Question using Google Data Studio. Each dashboard will feature a dropdown to change the GroupCategory type. The data will display multi-bar data along the Y-axis and the State on the X-axis.

## Running the project
TBD instructions
