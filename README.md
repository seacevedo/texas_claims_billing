

## Purpose

The Texas Department of Insurance, Division of Workers' Compensation (DWC) maintains a database of medical billing services. It contains charges, payments, and treatments billed by health care professionals and/or medical facilities that treat injured employees, including ambulatory surgical centers, with dates of service for the last five years. The data is separated into two different types of billing services:

* Professional Medical Billing Services (SV1) -  Charges, payments, and treatments billed on a CMS-1500 form by doctors and other health care professionals
* Institutional Medical Billing Services (SV2) - Charges, payments, and treatments billed on a CMS-1450 form (UB-92, UB-04) by hospitals and medical facilities

The data for each of these billing services are further seperated into a header (which groups individual line items within a bill) and detail information (individual line items a single bill). The purpose of this repository is to conceptualize relationships between entites in this dataset by outlining and implementing a data model. We will use a Star schema to model this dataset and then implement it using the data build tool (dbt). The idea would be to model this data in such a way within a data warehouse (Snowflake) that it is optimized for analytical reporting.





