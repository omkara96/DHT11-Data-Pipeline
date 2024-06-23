import params

for key in params.config:
    print(f"key: {key} and Value: {params.config.get(key)}")