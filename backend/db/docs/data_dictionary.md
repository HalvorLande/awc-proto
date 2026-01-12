# DealRadar Database Data Dictionary (dbo)
- Generated: **2026-01-12 12:26:32 UTC**
- Server: `AAD-GM12FD8W`
- Database: `AwcProto`
- Include row counts: `True`

---
## dbo.company
- Approx rows: **2417**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | varchar(9) | NO | NO |  |
| 2 | `name` | varchar(255) | NO | NO |  |
| 3 | `nace` | varchar(10) | YES | NO |  |
| 4 | `municipality` | varchar(100) | YES | NO |  |
| 5 | `website` | varchar(255) | YES | NO |  |
| 6 | `created_at` | datetime | NO | NO |  |
| 7 | `updated_at` | datetime | NO | NO |  |
| 8 | `phone` | nvarchar(50) | YES | NO |  |
| 9 | `email` | nvarchar(255) | YES | NO |  |
| 10 | `street` | nvarchar(255) | YES | NO |  |
| 11 | `postal_code` | nvarchar(20) | YES | NO |  |
| 12 | `city` | nvarchar(100) | YES | NO |  |
| 13 | `country_code` | char(2) | YES | NO |  |
| 14 | `sector_code` | nvarchar(50) | YES | NO |  |
| 15 | `is_public_sector` | bit | NO | NO | ((0)) |
| 16 | `excluded_reason` | nvarchar(200) | YES | NO |  |
| 17 | `last_proff_fetch_at_utc` | datetime2 | YES | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__company__3580CFE607436CF7`: (`orgnr`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__company__3580CFE607436CF7`: (`orgnr`)

---
## dbo.company_contact_person
- Approx rows: **10978**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `id` | int | NO | YES |  |
| 2 | `orgnr` | varchar(9) | NO | NO |  |
| 3 | `company_name` | nvarchar(255) | YES | NO |  |
| 4 | `person_name` | nvarchar(255) | NO | NO |  |
| 5 | `role` | nvarchar(100) | YES | NO |  |
| 6 | `started_date` | date | YES | NO |  |
| 7 | `phone` | nvarchar(50) | YES | NO |  |
| 8 | `email` | nvarchar(255) | YES | NO |  |
| 9 | `postal_address` | nvarchar(255) | YES | NO |  |
| 10 | `postal_postnr` | nvarchar(20) | YES | NO |  |
| 11 | `postal_city` | nvarchar(100) | YES | NO |  |
| 12 | `business_address` | nvarchar(255) | YES | NO |  |
| 13 | `business_postnr` | nvarchar(20) | YES | NO |  |
| 14 | `business_city` | nvarchar(100) | YES | NO |  |
| 15 | `revenue` | float | YES | NO |  |
| 16 | `employees` | int | YES | NO |  |
| 17 | `source_file` | nvarchar(260) | YES | NO |  |
| 18 | `imported_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |

### Key Constraints
- **PRIMARY KEY** `PK__company___3213E83F638BF838`: (`id`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__company___3213E83F638BF838`: (`id`)
- UNIQUE NONCLUSTERED `UX_contact_unique`: (`orgnr`, `person_name`, `role`, `started_date`)

---
## dbo.daily_top_pick
- Approx rows: **93**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `id` | int | NO | YES |  |
| 2 | `pick_date` | date | NO | NO |  |
| 3 | `rank` | int | NO | NO |  |
| 4 | `orgnr` | varchar(9) | NO | NO |  |
| 5 | `reason_summary` | varchar(500) | YES | NO |  |
| 6 | `total_score_snapshot` | float | NO | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__daily_to__3213E83FA2312BE7`: (`id`)
- **UNIQUE** `uq_pick_date_orgnr`: (`pick_date`, `orgnr`)
- **UNIQUE** `uq_pick_date_rank`: (`pick_date`, `rank`)

### Foreign Keys
- `FK__daily_top__orgnr__4316F928`: (`orgnr`) → `dbo.company` (`orgnr`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- NONCLUSTERED `ix_pick_date`: (`pick_date`)
- UNIQUE CLUSTERED `PK__daily_to__3213E83FA2312BE7`: (`id`)
- UNIQUE NONCLUSTERED `uq_pick_date_orgnr`: (`pick_date`, `orgnr`)
- UNIQUE NONCLUSTERED `uq_pick_date_rank`: (`pick_date`, `rank`)

---
## dbo.financial_statement
- Approx rows: **9487**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `id` | int | NO | YES |  |
| 2 | `orgnr` | varchar(9) | NO | NO |  |
| 3 | `year` | int | NO | NO |  |
| 4 | `revenue` | float | YES | NO |  |
| 5 | `ebitda` | float | YES | NO |  |
| 6 | `ebit` | float | YES | NO |  |
| 7 | `cfo` | float | YES | NO |  |
| 8 | `assets` | float | YES | NO |  |
| 9 | `equity` | float | YES | NO |  |
| 10 | `net_debt` | float | YES | NO |  |
| 11 | `source` | nvarchar(50) | NO | NO | ('proff') |
| 12 | `fetched_at_utc` | datetime2 | YES | NO |  |
| 13 | `account_view` | nvarchar(40) | NO | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__financia__3213E83F445E86B7`: (`id`)
- **UNIQUE** `uq_fin_orgnr_year`: (`orgnr`, `year`)

### Foreign Keys
- `FK__financial__orgnr__3A81B327`: (`orgnr`) → `dbo.company` (`orgnr`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- NONCLUSTERED `ix_fin_orgnr_year`: (`orgnr`, `year`)
- UNIQUE CLUSTERED `PK__financia__3213E83F445E86B7`: (`id`)
- UNIQUE NONCLUSTERED `uq_fin_orgnr_year`: (`orgnr`, `year`)
- UNIQUE NONCLUSTERED `UX_financial_statement_orgnr_year_view`: (`orgnr`, `year`, `account_view`)

---
## dbo.import_batch
- Approx rows: **1**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `batch_id` | int | NO | YES |  |
| 2 | `batch_name` | nvarchar(100) | NO | NO |  |
| 3 | `criteria` | nvarchar(1000) | YES | NO |  |
| 4 | `created_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |

### Key Constraints
- **PRIMARY KEY** `PK__import_b__DBFC0431C129F973`: (`batch_id`)
- **UNIQUE** `UQ__import_b__0E0738E90C5AB353`: (`batch_name`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__import_b__DBFC0431C129F973`: (`batch_id`)
- UNIQUE NONCLUSTERED `UQ__import_b__0E0738E90C5AB353`: (`batch_name`)

---
## dbo.import_batch_item
- Approx rows: **2439**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `batch_id` | int | NO | NO |  |
| 2 | `orgnr` | char(9) | NO | NO |  |
| 3 | `include_reason` | nvarchar(500) | YES | NO |  |
| 4 | `added_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |

### Key Constraints
- **PRIMARY KEY** `PK_import_batch_item`: (`batch_id`, `orgnr`)

### Foreign Keys
- `FK_import_batch_item_batch`: (`batch_id`) → `dbo.import_batch` (`batch_id`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- NONCLUSTERED `IX_import_batch_item_orgnr`: (`orgnr`)
- UNIQUE CLUSTERED `PK_import_batch_item`: (`batch_id`, `orgnr`)

---
## dbo.ingestion_checkpoint
- Approx rows: **7**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `run_id` | uniqueidentifier | NO | NO |  |
| 2 | `phase` | nvarchar(50) | NO | NO |  |
| 3 | `last_orgnr` | char(9) | YES | NO |  |
| 4 | `last_offset` | int | YES | NO |  |
| 5 | `last_cursor` | nvarchar(2000) | YES | NO |  |
| 6 | `updated_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |

### Key Constraints
- **PRIMARY KEY** `PK_ingestion_checkpoint`: (`run_id`, `phase`)

### Foreign Keys
- `FK_ingestion_checkpoint_run`: (`run_id`) → `dbo.ingestion_run` (`run_id`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- UNIQUE CLUSTERED `PK_ingestion_checkpoint`: (`run_id`, `phase`)

---
## dbo.ingestion_run
- Approx rows: **25**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `run_id` | uniqueidentifier | NO | NO | (newsequentialid()) |
| 2 | `run_type` | nvarchar(50) | NO | NO |  |
| 3 | `batch_name` | nvarchar(100) | YES | NO |  |
| 4 | `status` | nvarchar(20) | NO | NO | ('running') |
| 5 | `started_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |
| 6 | `finished_at_utc` | datetime2 | YES | NO |  |
| 7 | `notes` | nvarchar(2000) | YES | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK_ingestion_run`: (`run_id`)

### Foreign Keys
- _(none)_

### Indexes
- NONCLUSTERED `IX_ingestion_run_type_started`: (`run_type`, `started_at_utc`)
- UNIQUE CLUSTERED `PK_ingestion_run`: (`run_id`)

---
## dbo.outreach
- Approx rows: **0**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | varchar(9) | NO | NO |  |
| 2 | `owner` | varchar(100) | YES | NO |  |
| 3 | `status` | varchar(50) | NO | NO |  |
| 4 | `last_contact_at` | datetime | YES | NO |  |
| 5 | `next_step_at` | datetime | YES | NO |  |
| 6 | `note` | varchar(2000) | YES | NO |  |
| 7 | `updated_at` | datetime | NO | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__outreach__3580CFE62BE13B4A`: (`orgnr`)

### Foreign Keys
- `FK__outreach__orgnr__45F365D3`: (`orgnr`) → `dbo.company` (`orgnr`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- UNIQUE CLUSTERED `PK__outreach__3580CFE62BE13B4A`: (`orgnr`)

---
## dbo.proff_financial_item
- Approx rows: **91468**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | char(9) | NO | NO |  |
| 2 | `fiscal_year` | int | NO | NO |  |
| 3 | `account_view` | nvarchar(20) | NO | NO |  |
| 4 | `code` | nvarchar(80) | NO | NO |  |
| 5 | `value` | decimal(19,2) | YES | NO |  |
| 6 | `currency` | nvarchar(10) | YES | NO |  |
| 7 | `unit` | nvarchar(20) | YES | NO |  |
| 8 | `fetched_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |
| 9 | `source` | nvarchar(50) | NO | NO | ('proff') |

### Key Constraints
- **PRIMARY KEY** `PK_proff_financial_item`: (`orgnr`, `fiscal_year`, `account_view`, `code`)

### Foreign Keys
- _(none)_

### Indexes
- NONCLUSTERED `IX_proff_fin_item_orgnr_year`: (`orgnr`, `fiscal_year`)
- NONCLUSTERED `IX_proff_fin_item_year_code`: (`fiscal_year`, `code`)
- UNIQUE CLUSTERED `PK_proff_financial_item`: (`orgnr`, `fiscal_year`, `account_view`, `code`)

---
## dbo.proff_raw_company
- Approx rows: **0**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | char(9) | NO | NO |  |
| 2 | `fetched_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |
| 3 | `http_status` | int | NO | NO |  |
| 4 | `source_url` | nvarchar(800) | YES | NO |  |
| 5 | `etag` | nvarchar(200) | YES | NO |  |
| 6 | `payload_json` | nvarchar(MAX) | YES | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__proff_ra__3580CFE65E11F3EC`: (`orgnr`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__proff_ra__3580CFE65E11F3EC`: (`orgnr`)

---
## dbo.proff_raw_owners
- Approx rows: **0**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | char(9) | NO | NO |  |
| 2 | `fetched_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |
| 3 | `http_status` | int | NO | NO |  |
| 4 | `source_url` | nvarchar(800) | YES | NO |  |
| 5 | `etag` | nvarchar(200) | YES | NO |  |
| 6 | `payload_json` | nvarchar(MAX) | YES | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__proff_ra__3580CFE659D13977`: (`orgnr`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__proff_ra__3580CFE659D13977`: (`orgnr`)

---
## dbo.proff_raw_structure
- Approx rows: **0**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `orgnr` | char(9) | NO | NO |  |
| 2 | `fetched_at_utc` | datetime2 | NO | NO | (sysutcdatetime()) |
| 3 | `http_status` | int | NO | NO |  |
| 4 | `source_url` | nvarchar(800) | YES | NO |  |
| 5 | `etag` | nvarchar(200) | YES | NO |  |
| 6 | `payload_json` | nvarchar(MAX) | YES | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__proff_ra__3580CFE671DEDA49`: (`orgnr`)

### Foreign Keys
- _(none)_

### Indexes
- UNIQUE CLUSTERED `PK__proff_ra__3580CFE671DEDA49`: (`orgnr`)

---
## dbo.score
- Approx rows: **2417**
### Columns
| # | Name | Type | Nullable | Identity | Default |
|---:|---|---|:---:|:---:|---|
| 1 | `id` | int | NO | YES |  |
| 2 | `orgnr` | varchar(9) | NO | NO |  |
| 3 | `year` | int | NO | NO |  |
| 4 | `total_score` | float | NO | NO |  |
| 5 | `compounder_score` | float | NO | NO |  |
| 6 | `catalyst_score` | float | NO | NO |  |
| 7 | `tags` | varchar(500) | YES | NO |  |
| 8 | `computed_at` | datetime | NO | NO |  |

### Key Constraints
- **PRIMARY KEY** `PK__score__3213E83FFDE70548`: (`id`)
- **UNIQUE** `uq_score_orgnr_year`: (`orgnr`, `year`)

### Foreign Keys
- `FK__score__orgnr__3E52440B`: (`orgnr`) → `dbo.company` (`orgnr`) [ON DELETE NO_ACTION, ON UPDATE NO_ACTION]

### Indexes
- NONCLUSTERED `ix_score_orgnr_year`: (`orgnr`, `year`)
- NONCLUSTERED `ix_score_total`: (`total_score`)
- UNIQUE CLUSTERED `PK__score__3213E83FFDE70548`: (`id`)
- UNIQUE NONCLUSTERED `uq_score_orgnr_year`: (`orgnr`, `year`)
- UNIQUE NONCLUSTERED `UX_score_orgnr_year`: (`orgnr`, `year`)

---
