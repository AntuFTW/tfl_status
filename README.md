# TFL Crowding Project

This is a small project to showcase my Dagster learning. It is a small ETL pipeline which requests data from the TFL API about the crowding level of different tube stations. This is done for all station every 10 minutes. This data is then stored in a DuckDB database. Every day at 9am, figures are created to showcase the crowding levels of different tube stations and these are saved locally.

All the scheduling and jobs have been done through Dagster!

