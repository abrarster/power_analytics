from dagster import MonthlyPartitionsDefinition, DailyPartitionsDefinition

monthly_partition = MonthlyPartitionsDefinition(start_date='2018-01-01')
