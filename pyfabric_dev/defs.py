"""
Framework-level constants shared across all pipeline layers.

Medallion-architecture lakehouse names, default paths, and a
Grandfather-Father-Son backup-retention policy. Re-export or extend
these in your consumer project's own ``defs`` module to add
project-specific table names.
"""

# Default paths
DEFAULT_LAKEHOUSE_PATH = "/lakehouse/default"
DEFAULT_BACKUP_TABLE_REFRESH_PERIOD_DAYS = 1

# Lakehouse names (medallion architecture convention)
BRONZE_LAKEHOUSE_NAME = "bronze_lakehouse"
SILVER_LAKEHOUSE_NAME = "silver_lakehouse"
GOLD_LAKEHOUSE_NAME = "gold_lakehouse"
BACKUP_LAKEHOUSE_NAME = "backup_lakehouse"

# Backup retention (GFS): keep N most-recent daily, N weekly (Sunday), N monthly (1st-of-month).
BACKUP_RETENTION_DAILY = 14
BACKUP_RETENTION_WEEKLY = 8
BACKUP_RETENTION_MONTHLY = 12
