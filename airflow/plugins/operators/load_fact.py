from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    load_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,

                 redshift_conn_id="",
                 table="",
                 load_fact_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_fact_sql = load_fact_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading the fact table {}".format(self.table))
        formatted_sql = LoadFactOperator.load_sql.format(
            self.table, 
            self.load_fact_sql
        )
        redshift.run(formatted_sql)
            
            
