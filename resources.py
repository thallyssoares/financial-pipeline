from dagster import FilesystemIOManager

save_csv = FilesystemIOManager(base_dir="data")
