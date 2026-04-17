
```
analytics/dbt/bts_airline/
├── analyses/        ← Generally use it for data quality reports. (Temp analyses)
├── macros/          ← They behave like Python Functions (reusable logic)                
├── models/
│   ├── staging/     ← ​​converting source data (raw data) to DBT standards.
│   │                  only column names are corrected.
│   └── mart/        ← This is the final, cleaned and enriched data tables to used by business units (BI AND DA) are created.
├── seeds/           ← A space to upload csv files (lookup files)
├── snapshots/       ← slowly changing dimension following (dont used this project)
│                      It logs changes in the data in a table over time.
├── tests/           ← This is where data quality tests specific to the business logic are written.
├── dbt_project.yml  ← The most important file in dbt. Project configuration, model materialization settings
└── README.md        ← Description.
```

