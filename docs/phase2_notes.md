# Phase 2 Notes (Bronze)

## NOAA "details" table
- The Glue crawler-generated table `bronze_hazard.details` was invalid in Athena due to duplicate column names in the catalog metadata.
- Fix: dropped the crawler table and created a manually defined external table `bronze_hazard.details_raw` pointing to the same S3 location.
- Partitions loaded via `MSCK REPAIR TABLE bronze_hazard.details_raw;`
