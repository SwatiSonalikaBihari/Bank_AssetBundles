Bank Loan Modelling – Delta Live Tables Pipeline (DLT)

This project implements an end-to-end Medallion Architecture using Databricks Delta Live Tables (DLT).
It ingests raw CSV data, performs structured transformations across Bronze → Silver → Gold layers, applies SCD Type-1, builds dimensional and fact tables, and finally produces a Star Schema for analytical consumption.

          ┌──────────┐
         │   CSV     │
         └─────┬────┘
               │
               ▼
        ┌────────────┐
        │  BRONZE     │
        └─────┬──────┘
               │
               ▼
        ┌────────────┐
        │  SILVER     │
        └─────┬──────┘
               │ STREAM
               ▼
        ┌────────────┐
        │ SCD TYPE 1  │
        └─────┬──────┘
               │
               ▼
        ┌───────────────────────────────┐
        │              GOLD              │
        │                                 │
        │    ┌────────┐   ┌────────┐     │
        │    │ DIM 1  │   │ DIM 2  │     │
        │    └────┬───┘   └───┬────┘     │
        │         │           │           │
        │         ▼           ▼           │
        │        ┌──────── FACT ────────┐ │
        │        │                      │ │
        │        └──────────┬───────────┘ │
        │                   │             │
        │                   ▼             │
        │            ┌────────────┐       │
        │            │   STAR     │       │
        │            │  SCHEMA    │       │
        │            └────────────┘       │
        └─────────────────────────────────┘
        Key Features :-
        1.Implemented using Delta Live Tables (DLT)
        2.Streaming pipeline from Silver → SCD1
        3.SCD Type-1 using dlt.apply_changes()
        4.Bronze → Silver → Gold Medallion Architecture
        5.Star Schema with:
           fact_loan

           dim_customer

           dim_account

           loan_star_schema
         6.Data Quality constraints using EXPECT_OR_DROP
