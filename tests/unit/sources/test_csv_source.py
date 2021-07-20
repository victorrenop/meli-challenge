from meli_challenge.sources import CSVSource

BASE_TEST_FILE_FOLDER = "tests/unit/sources/test_data/csv_data/%s"
BASE_SCHEMA_FILE = BASE_TEST_FILE_FOLDER % ("base_schema.json")


class TestCSVSource:
    def test_read_single_file(self, spark_session, expected_result):
        csv_source = CSVSource(
            spark_session=spark_session,
            source_files=BASE_TEST_FILE_FOLDER % ("base_test_file.csv"),
            read_options={
                "header": "true",
                "inferSchema": False,
                "delimiter": ",",
            },
        )

        actual_result = csv_source.read(BASE_SCHEMA_FILE).collect()

        assert expected_result == actual_result

    def test_read_multiple_files(self, spark_session, expected_result):
        csv_source = CSVSource(
            spark_session=spark_session,
            source_files=[
                BASE_TEST_FILE_FOLDER % ("base_test_file_part_1.csv"),
                BASE_TEST_FILE_FOLDER % ("base_test_file_part_2.csv"),
            ],
            read_options={
                "header": "true",
                "inferSchema": False,
                "delimiter": ",",
            },
        )

        actual_result = csv_source.read(BASE_SCHEMA_FILE).collect()

        assert expected_result == actual_result

    def test_read_base_path(self, spark_session, expected_result):
        csv_source = CSVSource(
            spark_session=spark_session,
            source_files=BASE_TEST_FILE_FOLDER % ("base_csv_folder/"),
            base_path=BASE_TEST_FILE_FOLDER % ("base_csv_folder/"),
            read_options={
                "header": "true",
                "inferSchema": False,
                "delimiter": ",",
            },
        )

        actual_result = csv_source.read(BASE_SCHEMA_FILE).collect()
        actual_result.sort(key=lambda x: x["id"])

        assert expected_result == actual_result

    def test_read_no_header_file(self, spark_session, expected_result):
        csv_source = CSVSource(
            spark_session=spark_session,
            source_files=BASE_TEST_FILE_FOLDER % ("no_header_file.csv"),
            read_options={
                "header": "false",
                "inferSchema": False,
                "delimiter": ",",
            },
        )

        actual_result = csv_source.read(BASE_SCHEMA_FILE).collect()

        assert expected_result == actual_result
