

## Purpose

The Texas Department of Insurance, Division of Workers' Compensation (DWC) maintains a database of medical billing services. It contains charges, payments, and treatments billed by health care professionals and/or medical facilities that treat injured employees, including ambulatory surgical centers, with dates of service for the last five years. The data is separated into two different types of billing services:

* Professional Medical Billing Services (SV1) -  Charges, payments, and treatments billed on a CMS-1500 form by doctors and other health care professionals
* Institutional Medical Billing Services (SV2) - Charges, payments, and treatments billed on a CMS-1450 form (UB-92, UB-04) by hospitals and medical facilities

The data for each of these billing services are further seperated into a header (which groups individual line items within a bill) and detail information (individual line items a single bill). The purpose of this repository is to conceptualize relationships between entites in this dataset by outlining and implementing a data model. We will use a Star schema to model this dataset and then implement it using the data build tool (dbt). The idea would be to model this data in such a way within a data warehouse (Snowflake) that it is optimized for analytical reporting. 

## Technology Stack

* [Google Drive](https://www.google.com/drive/) - Staging area where data is uploaded to prepare for transfer to Data Warehouse
* [Fivetran](https://www.fivetran.com/) - Extracts and Loads data from Google drive into Snowflake
* [Snowflake](https://www.snowflake.com/en/) - Act as our Data Warehouse
* [dbt](https://www.getdbt.com/) - Transforms raw data from Snowflake and constructs Data Warehouse Layers

## Architecture
![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/texas_claims_architecture_diagram.png)

* Data from the Texas open data portal is downloaded and dumped into Google drive as Google sheets. This data corresponds to billing claims made within the month of October 2023. Fivetran extracts the data from Google drive and loads it into Snowflake.
* Raw data is dumped into Snowflake as a database created by Fivetran. Within the database, four new Schemas are created using the corresponding CSV files: INST_BILLING_HEADER, INST_BILLING_DETAIL, PROF_BILLING_HEADER, and PROF_BILLING_DETAIL
* dbt then is used perform tranformations on the loaded data. The raw data is used to create staging layer, where each column is cast to the appropriate data type. Data from the staging layer is then used to produce the Data Warehouse layer, where the data is modeled as a Star schema and contains the following tables: dim_date, dim_employee, dim_employer, dim_facility, dim_insurer, dim_provider, dim_service, and fct_claims_billing. Finally, we use the data from the newly constructed Data Warehouse layer to create two data marts in One Big Table (OBT) format: obt_claims_billing and obt_clinical_outcomes. 


