import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import sqlite3


def get_db_connection():
    return sqlite3.connect("sample_database.db")


def record_exists_in_database(record, conn):
    cursor = conn.cursor()
    placeholders = ', '.join([f"{col} = ?" for col in record.keys()])
    query = f"SELECT 1 FROM imported_data WHERE {placeholders} LIMIT 1"
    cursor.execute(query, tuple(record.values()))
    return cursor.fetchone() is not None


@pytest.fixture(scope="module")
def setup():
    driver = webdriver.Chrome()
    driver.implicitly_wait(10)
    yield driver
    driver.quit()


@pytest.fixture(scope="module")
def db_connection():
    conn = get_db_connection()
    yield conn
    conn.close()

def upload_csv(driver, file_path):
    driver.get("http://Netzsch_CSV_Import.com/upload")
    driver.find_element(By.ID, "csv-upload").send_keys(file_path)
    driver.find_element(By.ID, "submit-upload").click()
    time.sleep(3)


def test_valid_csv_import(setup, db_connection):
    driver = setup
    csv_path = "test_files/valid_data.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for valid CSV"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record, db_connection), "Record not found in database"


def test_missing_optional_fields(setup, db_connection):
    driver = setup
    csv_path = "test_files/missing_optional_fields.csv"  
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for CSV with optional NULL fields"

    
    expected_record = {
        "mandatory_column1": "value1",  # Mandatory field with a value
        "optional_column": None,  # Optional field left blank, expecting NULL in database
        "mandatory_column2": "value2"
    }

    assert record_exists_in_database(expected_record,
                                     db_connection), "Record with NULL optional fields not found in database"


def test_missing_required_fields(setup, db_connection):
    driver = setup
    csv_path = "test_files/missing_required_fields.csv"
    upload_csv(driver, csv_path)
    assert "Required fields missing" in driver.page_source, "Error message missing for missing fields"


def test_empty_csv_file(setup):
    driver = setup
    csv_path = "test_files/empty_file.csv"
    upload_csv(driver, csv_path)
    assert "No data to import" in driver.page_source, "Message missing for empty CSV file"


def test_duplicate_csv_file(setup, db_connection):
    driver = setup
    csv_path = "test_files/valid_data.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing on first import"
    upload_csv(driver, csv_path)
    assert "Duplicate data detected" in driver.page_source, "Duplicate data message missing on re-import"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record, db_connection), "Record should still be in database"


def test_duplicate_rows_in_same_csv(setup, db_connection):
    driver = setup
    csv_path = "test_files/duplicate_rows.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for duplicate rows"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record, db_connection), "Record not found in database with duplicate rows"


def test_incorrect_file_format(setup):
    driver = setup
    file_path = "test_files/invalid_format.txt"
    upload_csv(driver, file_path)
    assert "Invalid file format" in driver.page_source, "Error message missing for non-CSV file"


def test_csv_with_extra_columns(setup, db_connection):
    driver = setup
    csv_path = "test_files/extra_columns.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for extra columns"
    sample_record = {"column1": "value1", "column2": "value2"}  #
    assert record_exists_in_database(sample_record, db_connection), "Record with extra columns not found in database"


def test_missing_headers(setup):
    driver = setup
    csv_path = "test_files/missing_headers.csv"
    upload_csv(driver, csv_path)
    assert "Missing headers" in driver.page_source, "Error message missing for CSV without headers"


def test_duplicate_headers(setup):
    driver = setup
    csv_path = "test_files/duplicate_headers.csv"
    upload_csv(driver, csv_path)
    assert "Duplicate headers detected" in driver.page_source, "Error message missing for duplicate headers in CSV"


def test_incorrect_column_order(setup, db_connection):
    driver = setup
    csv_path = "test_files/incorrect_column_order.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for CSV with incorrect column order"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record,
                                     db_connection), "Record not found in database with incorrect column order"


def test_corrupted_csv_file(setup):
    driver = setup
    csv_path = "test_files/corrupted_file.csv"
    upload_csv(driver, csv_path)
    assert "Unable to read file" in driver.page_source, "Error message missing for corrupted CSV file"


def test_special_characters_utf8_encoding(setup, db_connection):
    driver = setup
    csv_path = "test_files/special_characters_utf8.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for CSV with UTF-8 special characters"
    sample_record = {"column1": "特殊字符", "column2": "éxample"}
    assert record_exists_in_database(sample_record,
                                     db_connection), "Record with special characters not found in database"


def test_column_mapping(setup, db_connection):
    driver = setup
    csv_path = "test_files/column_mapping.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for column mapping"
    expected_record = {
        "db_column1": "mapped_value1",
        "db_column2": "mapped_value2"
    }
    assert record_exists_in_database(expected_record,
                                     db_connection), "Record not found in database with correct column mapping"


def test_invalid_data_type_mismatch(setup):
    driver = setup
    csv_path = "test_files/invalid_data_type.csv"
    upload_csv(driver, csv_path)
    assert "Invalid data type" in driver.page_source, "Error message missing for data type mismatch"


def test_date_format(setup):
    driver = setup
    csv_path = "test_files/invalid_date_format.csv"
    upload_csv(driver, csv_path)
    assert "Invalid date format" in driver.page_source, "Error message missing for incorrect date format"


def test_time_format(setup):
    driver = setup
    csv_path = "test_files/invalid_time_format.csv"
    upload_csv(driver, csv_path)
    assert "Invalid time format" in driver.page_source, "Error message missing for incorrect time format"


def test_partial_data_errors(setup, db_connection):
    driver = setup
    csv_path = "test_files/partial_data_errors.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful with errors" in driver.page_source, "Expected partial import success message"
    valid_record = {"column1": "valid_value1", "column2": "valid_value2"}  # Valid row data
    assert record_exists_in_database(valid_record, db_connection), "Valid record not found in database"


def test_header_case_sensitivity(setup):
    driver = setup
    csv_path = "test_files/header_case_sensitivity.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for case-sensitive headers"


def test_partial_header_match(setup):
    driver = setup
    csv_path = "test_files/partial_header_match.csv"
    upload_csv(driver, csv_path)
    assert "Header mismatch detected" in driver.page_source, "Error message missing for header spelling mismatches"


def test_import_small_csv(setup, db_connection):
    driver = setup
    csv_path = "test_files/small_csv.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for small CSV file"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record, db_connection), "Small CSV record not found in database"


def test_import_large_csv(setup, db_connection):
    driver = setup
    csv_path = "test_files/large_csv.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for large CSV file"

    sample_record = {"column1": "max_value1", "column2": "max_value2"}
    assert record_exists_in_database(sample_record, db_connection), "Record from large CSV not found in database"


def test_maximum_character_length(setup):
    driver = setup
    csv_path = "test_files/max_character_length.csv"
    upload_csv(driver, csv_path)
    assert "Character length exceeded" in driver.page_source, "Error message missing for character length violations"


def test_maximum_integer_length(setup):
    driver = setup
    csv_path = "test_files/max_integer_length.csv"
    upload_csv(driver, csv_path)
    assert "Integer length exceeded" in driver.page_source, "Error message missing for integer length violations"


def test_embedded_delimiter_in_fields(setup, db_connection):
    driver = setup
    csv_path = "test_files/embedded_delimiter.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for embedded delimiters"
    sample_record = {"column1": "value, with comma", "column2": "another value"}
    assert record_exists_in_database(sample_record,
                                     db_connection), "Record with embedded delimiter not found in database"


def test_invalid_delimiter(setup):
    driver = setup
    csv_path = "test_files/invalid_delimiter.csv"
    upload_csv(driver, csv_path)
    assert "Invalid delimiter detected" in driver.page_source, "Error message missing for invalid delimiter"


def test_leading_and_trailing_whitespaces(setup, db_connection):
    driver = setup
    csv_path = "test_files/leading_trailing_whitespaces.csv"
    upload_csv(driver, csv_path)
    assert "Import Successful" in driver.page_source, "Success message missing for whitespace handling"
    sample_record = {"column1": "value1", "column2": "value2"}
    assert record_exists_in_database(sample_record, db_connection), "Record with whitespaces not found in database"


def test_small_file_import_time(setup):
    driver = setup
    csv_path = "test_files/small_csv.csv"
    start_time = time.time()
    upload_csv(driver, csv_path)
    end_time = time.time()
    assert "Import Successful" in driver.page_source, "Success message missing for small CSV import"
    import_time = end_time - start_time
    assert import_time < 5, f"Small file import took too long: {import_time} seconds"


def test_large_file_import_time(setup):
    driver = setup
    csv_path = "test_files/large_csv.csv"
    start_time = time.time()
    upload_csv(driver, csv_path)
    end_time = time.time()
    assert "Import Successful" in driver.page_source, "Success message missing for large CSV import"
    import_time = end_time - start_time
    assert import_time < 60, f"Large file import took too long: {import_time} seconds"
    driver.quit()
