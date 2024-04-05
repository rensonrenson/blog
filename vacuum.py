from delta.tables import DeltaTable
class DeltaTableVacuum:
    def __init__(self,  location, path, table_list):
        self.location = location
        self.path = path
        self.input_list = table_list

    def create_dict_with_day_to_hours(self):
        table_retention_dict = {}
        for i in range(0, len(self.input_list), 2):
            table_name = self.input_list[i]
            full_table_path = f"{self.path}{table_name}"
            value = self.input_list[i + 1]
            table_retention_dict[full_table_path] = value * 24
        return table_retention_dict

    def vacuum_delta_table(self):
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        for delta_path, retention_hours in obj.create_dict_with_day_to_hours().items():
            delta_table = DeltaTable.forPath(spark, delta_path)
            delta_table.vacuum(retention_hours)




