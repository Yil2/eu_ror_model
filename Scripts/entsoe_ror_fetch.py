
from entsoe import EntsoePandasClient


api_key = "your_api_key"
client = EntsoePandasClient(api_key=api_key)

area="your_electricity_price_area"
start_time="your_ror_start_time"
end_time="your_ror_end_time"
data_path="your_ror_data_path"

ror_requested=client.query_generation(area, start=start_time, end=end_time,
                            psr_type="B11")
ror_requested.to_csv(data_path)
print(f"Retrieve run-of-river generation data: {area} ---> Finished")