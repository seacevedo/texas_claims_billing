

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
* dbt then is used perform tranformations on the loaded data. The raw data is used to create staging layer, where each column is cast to the appropriate data type. Data from the staging layer is then used to produce the Data Warehouse layer, where the data is modeled as a Star schema and contains the following tables: `dim_date`, `dim_employee`, `dim_employer`, `dim_facility`, `dim_insurer`, `dim_provider`, `dim_service`, and `fct_claims_billing`. Finally, we use the data from the newly constructed Data Warehouse layer to create two data marts in One Big Table (OBT) format: `obt_claims_billing` and `obt_clinical_outcomes`.

## Data Model

### Requirements Gathering

To understand what dimensions we would need to create an Star schema that is appropriate for our dataset, we first outlined a list of business processes that would involve certain dimensions that would be needed to generate analytical reports. This can be summarized in the following Bus Matrix, where the columns denote the entities and the rows are the business processes of interest:

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/texas_claims_bus_matrix.png)

From this matrix, it is clear that we would need the follwing dimensions for our business needs: Employee, Provider, Date, Facility, Service, Employer, Insurer. To simplify things, we will focus our attention to the first two processes: Claims Billing Overview and Clinical Outcomes, to generate two data marts for our reporting layer. 

We can further expand the above Bus Matrix to describe the fact tables that will be necessary to describe each business processes, as well their respective grain. Both the Claims Billing Overview and Clinical Outcomes will share the fct_claims_billing table. The granularity for these processes will be transactional: the Claims Billing Overview business process will require a grain of one row per line within a bill, while the Clinical Outcomes process will need a grain of one row per procedure perfored on an employee. This is summarized in the following Bus Matrix:

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/texas_claims_bus_matrix_detailed.png)

### Conceptual Model

We begin by outling the conceptual data model. As described before, the dimensions of the Star schema are  `Employee`, `Provider`, `Date`, `Facility`, `Service`, `Employer`, `Insurer`. The only fact table we will have in our model is the `fct_claims_billing table`, which will have a relationship with each of the dimension tables.

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/conceptual_model_texas_claims.png)

### Logical Model

We now extend the previous conceptual data model by adding the cardinality of the relationships between the dimension and fact tables, as well as define all of the necessary columns associated with each entity. Each dimension table has a one to many relationship with the fact table, meaning with that each dimension can be associated with multiple facts.

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/logical_model_texas_claims.png)

### Physical Model

We continue to add layers to our existing data model. This time, we detail the data types for each column in every table. This is our final data model and what we will implement in Snowflake.

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/physical_model_texas_claims.png)

## dbt Implementation

We now describe the resulting dbt implementation, and outline in detail how we implemented our components in our Data Warehouse Layers. The following figure gives an overview of the Lineage graph.

![alt_text](https://github.com/seacevedo/texas_claims_billing/blob/main/assets/dbt_lineage_graph.png)

We will outline each model shown in the above Lineage graph. First we describe our staging models:

#### Staging Layer
* `stg_inst_billing_header`: Derived from the INST_BILLING_HEADER source table. Type casting is applied where columns are cast to appropriate data type. 
* `stg_inst_billing_detail`: Derived from the INST_BILLING_DETAIL source table. Type casting is applied where columns are cast to appropriate data type. 
* `stg_prof_billing_header`: Derived from the PROF_BILLING_HEADER source table. Type casting is applied where columns are cast to appropriate data type. 
* `stg_prof_billing_detail`: Derived from the PROF_BILLING_DETAIL source table. Type casting is applied where columns are cast to appropriate data type.

To help make our final data models more readable, we seperate out some of our logic into three intermediate models:

#### Intermediate Layer

* `int_detail_union`: Derived from the `stg_inst_billing_detail` and `stg_prof_billing_detail` models. We perform a union join on both models and select only unique rows.
* `int_header_union`: Derived from the `stg_inst_billing_header` and `stg_prof_billing_header` models. We perform a union join on both models and select only unique rows.
* `int_provider_consolidate`: Solely derived from `int_header_union`. In this dataset, three types of provider are present: `billing` (submits insurance claims for reimbursement), `rendering` (individual that renders the m edical service, usually some sort of doctor), and `referring` (person who directed the patient to the rendering provider). Here we consolidate the data from all three providers by seperating it out into Common Table Expressions (CTEs) and performing a union join on all three CTEs, selecting only unique rows.

We will then turn our attention to our Data Warehouse Layer, where we will finally implement our Star Schema outlined above. It contains eight data models, each corresponding to the tables we described in our data model:

#### Data Warehouse Layer

* `dim_date`: Derived from `int_detail_union`. Unique dates are selected and data like month, day, quarter, etc are extracted from each date.
* `dim_employee`: Derived from `int_header_union`. The model is built by selecting only columns relating to employee data. 
* `dim_employer`: Derived from `int_header_union`. The model is built by selecting only columns relating to employer data. 
* `dim_facility`: Derived from `int_header_union`. The model is built by selecting only columns relating to medical facility data. 
* `dim_insurer`: Derived from `int_header_union`. The model is built by selecting only columns relating to insurer data. 
* `dim_provider`: Derived from `int_provider_consolidate`. The model is built by selecting only columns relating to provider data. 
* `dim_service`: Derived from `int_header_union` and `int_detail_union`. The model is built by creating CTEs of both of these models and left join on them. Only unique rows are then selected.
* `fct_claims_billing`: Derived from `int_header_union`, `int_detail_union`, and `int_provider_consolidate`. The model is built by creating CTEs of all three models and performing left joins on them.

Finally, we will describe our Data Marts layer, which can be used for reporting purposes. As mentioned previosuly, we will focus on developing data marts for two business processes: `Claims Billing Overview` and `Clinical Outcomes`.

#### Data Marts Layer

* `obt_claims_billing`: Derived from `dim_insurer`, `dim_date`, and `fct_claims_billing`. We first perform an inner join on all three tables, and select only unique rows. We then redefine the grain of our resulting table by summing the total bill charge amount, payment amount, days charged, and days paid for each day.
* `obt_clinical_outcomes`: Derived from `dim_date`, `dim_employee`, `dim_facility`, `dim_provider`, `dim_service`, and `fct_claims_billing`. The model is built by performing and inner join on all three tables. Unlike `obt_claims_billing` the grain is not redefined: rather it remains the same as `fct_claims_billing`, where each row corresponds to one line item within a bill. In this case you can think of the grain as a single procedure listed within the line item of a bill.

