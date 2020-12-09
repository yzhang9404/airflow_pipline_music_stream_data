from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_append = """
        INSERT INTO {}
        {};
        COMMIT;
    """
    sql_delete = """
        TRUNCATE TABLE {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_data=True,
                 load_dimension_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_data = append_data
        self.load_dimension_sql = load_dimension_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            self.log.info("Loading the dimension table {}".format(self.table))
            sql_statement = LoadDimensionOperator.sql_append.format(
            self.table, 
            self.load_dimension_sql
            )   
            redshift.run(sql_statement)
        else:
            self.log.info("Clearing data from destination Redshift table and Loading the dimension table {}".format(self.table))
            sql_statement = LoadDimensionOperator.sql_delete.format(
            self.table, 
            self.table,
            self.load_dimension_sql
            )  
            redshift.run(sql_statement)