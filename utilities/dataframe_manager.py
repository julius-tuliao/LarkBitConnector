import pandas as pd


class DataframeManager:
    def __init__(self, max_retries=3):
        self.max_retries = max_retries

    def get_dataframe_from_records(self,records, field_map):
        rows = []

        for _ in range(self.max_retries):

            try:
                for record in records:
                    row_data = {}
                    for key, value in field_map.items():
                        if isinstance(value, list):
                            nested_value = record.get('fields', {}).get(value[0], [{}])[0].get(value[1], "")
                            row_data[key] = nested_value
                        else:
                            row_data[key] = record.get('fields', {}).get(value, "")
                    rows.append(row_data)
                return pd.DataFrame(rows)
            
            except Exception as e:
                print(f"Error: {e}")
        return None