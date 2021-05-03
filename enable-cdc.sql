EXEC sys.sp_cdc_enable_db

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'produtos',
@role_name = N'public',
@filegroup_name = N'PRIMARY',
@supports_net_changes = 0

EXEC sys.sp_cdc_help_change_data_capture