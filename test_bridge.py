import eth_lake

result = eth_lake.sum_as_string(5, 7)

print(f"Result from Rust: {result}")
print(f"Type of result: {type(result)}")