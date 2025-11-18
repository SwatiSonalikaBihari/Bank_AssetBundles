import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# BRONZE 

@dlt.table(
    table_properties={"quality": "bronze"},
    comment="Raw Bank data ingested from CSV source"
)
def bank_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/databricks_bank_loan_modelling_dataset/v01/banking/loan-clean.csv")
    )

    df = (df
        .withColumnRenamed("ZIP Code", "zip_code")
        .withColumnRenamed("Personal Loan", "Personal_Loan")
        .withColumnRenamed("Securities Account", "Securities_Account")
        .withColumnRenamed("CD Account", "CD_Account")
    )
    return df


# SILVER

@dlt.table(
    name="bank_silver",
    comment="Cleaned bank data with casting, transformations and null checks",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_id", "ID IS NOT NULL")
@dlt.expect_or_drop("valid_income", "Income IS NOT NULL")
def bank_silver_table():   
    return (
        dlt.read("bank_bronze")
        .fillna("NA")
        .fillna(0)
    )


# SCD TYPE-1 

#  streaming table for SCD1
dlt.create_streaming_table("Bank_SCD1")

@dlt.view(name="bank_silver_vw")
def bank_silver_vw():
    return dlt.read_stream("bank_silver")


dlt.apply_changes(
    target="Bank_SCD1",
    source="bank_silver_vw",
    keys=["ID"],
    stored_as_scd_type=1,
    sequence_by=col("ID")
)

#  GOLD 

@dlt.table(
    name="bank_gold_income_by_age",
    comment="Income metrics grouped by Age",
    table_properties={"quality": "gold"}
)
def bank_gold_income_by_age():
    return (
        dlt.read("Bank_SCD1")
        .groupBy("Age")
        .agg(
            avg("Income").alias("avg_income"),
            min("Income").alias("min_income"),
            max("Income").alias("max_income"),
            count("*").alias("total_customers")
        )
        .orderBy("Age")
    )


@dlt.table(
    name="bank_gold_creditcards_per_family",
    comment="Number of credit cards grouped by family",
    table_properties={"quality": "gold"}
)
def bank_gold_creditcards_per_family():
    return (
        dlt.read("Bank_SCD1")
        .groupBy("Family")
        .agg(
            sum("CreditCard").alias("total_credit_cards"),
            count("*").alias("total_customers")
        )
        .orderBy("Family")
    )

#  DIMENTIONAL TABLES 

# DIM CUSTOMER
@dlt.table(
    name="dim_customer",
    comment="Customer dimension with personal details",
    table_properties={"quality": "gold"}
)
def dim_customer():
    return (
        dlt.read("Bank_SCD1")
        .select(
            col("ID").alias("customer_key"),
            "Age",
            "Experience",
            "Family",
            "Education",
            "zip_code"
        )
        .dropDuplicates(["customer_key"])
    )


# DIM ACCOUNT
@dlt.table(
    name="dim_account",
    comment="Account dimension with financial product details",
    table_properties={"quality": "gold"}
)
def dim_account():
    return (
        dlt.read("Bank_SCD1")
        .select(
            col("ID").alias("customer_key"),
            "CD_Account",
            "Securities_Account",
            "Online",
            "CreditCard"
        )
        .dropDuplicates(["customer_key"])
    )

#  FACT TABLE ------------------------


@dlt.table(
    name="fact_loan",
    comment="Fact table capturing customer loan performance",
    table_properties={"quality": "gold"}
)
def fact_loan():
    df = dlt.read("Bank_SCD1")
    return (
        df.select(
            col("ID").alias("customer_key"),
            "Income",
            "Mortgage",
            "CCAvg",
            "Personal_Loan"
        )
    )



#  STAR SCHEMA 

@dlt.table(
    name="loan_star_schema",
    comment="Full star schema model for bank loan analytics",
    table_properties={"quality": "gold"}
)
def loan_star_schema():
    fact = dlt.read("fact_loan")
    dim_cust = dlt.read("dim_customer")
    dim_acc = dlt.read("dim_account")

    return (
        fact
        .join(dim_cust, "customer_key", "left")
        .join(dim_acc, "customer_key", "left")
    )
