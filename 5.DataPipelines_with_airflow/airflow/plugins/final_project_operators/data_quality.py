from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        self.log.info('DataQualityOperator is running data quality checks')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not isinstance(self.tests, list) or len(self.tests) == 0:
            self.log.error("Data quality check failed: no tests provided")
            raise ValueError("Data quality check failed: no tests provided")
        
        failed_tests = []
        for test in self.tests:
            sql_check = test.get('check_sql')
            expected_result = test.get('expected_result')
            
            if not sql_check or expected_result is None:
                self.log.warning("Skipping a test due to missing 'check_sql' or 'expected_result'.")
                continue
            
            self.log.info(f"Running data quality check: {sql_check}")
            records = redshift_hook.get_records(sql_check)
            
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] != expected_result:
                failed_tests.append(sql_check)
                
        if len(failed_tests) > 0:
            self.log.error("Data quality checks failed.")
            self.log.error("Failed checks: %s", failed_tests)
            raise ValueError("Data quality check failed.")
        else:
            self.log.info("All data quality checks passed successfully")