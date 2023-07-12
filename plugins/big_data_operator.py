from airflow.models import BaseOperator
from airflow.utils.context import Context
import pandas as pd


class BigDataOperator(BaseOperator):
    def __init__(
        self,
        path_to_csv,
        path_to_save_file,
        sep=";",
        file_type="parquet",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.path_to_csv = path_to_csv
        self.path_to_save_file = path_to_save_file
        self.sep = sep
        self.file_type = file_type

    def execute(self, context: Context):
        df = pd.read_csv(self.path_to_csv, sep=self.sep)
        if self.file_type == "parquet":
            df.to_parquet(self.path_to_save_file)
        elif self.file_type == "json":
            df.to_json(self.path_to_save_file)
        else:
            raise ValueError("O tipo de arquivo para salvar é inválido!")
