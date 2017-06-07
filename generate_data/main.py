"""
Insight Data Engineering
Kyle Schmidt

Main method to generate user data
"""
import csv
from datetime import datetime
import os
import re
from typing import Generator, Tuple

from faker import Faker  # type: ignore

from config import PATH_INSTANCE, s3_client


def remove_non_alpha_chars(string: str) -> str:
    """
    Remove non-alphabetical characters from given string

    Arguments:
        string: String to replace chars

    Returns:
        string without non-alphabetical characters
    """
    regex = re.compile('[^a-zA-Z]')
    return regex.sub('', string)


def gen_fake_users(num_fakes: int) -> Generator[Tuple[str, str], None, None]:
    """
    Generate Fake Users

    Arguments:
        num_fakes: Number of fake users to generate
    """
    fake = Faker()
    for _ in range(num_fakes):
        full_name = fake.name()  # pylint: disable=no-member
        name = remove_non_alpha_chars(full_name).lower()
        yield name, full_name


def create_s3_key(tablename: str,
                  local_filename: str) -> str:
    """
    Create the S3 Key Name

    Arguments:
        tablename: Name of table in S3
        local_filename: Path to local file
        date_today: String representation of today's date

    Returns:
        String formatted S3 Key
    """
    date_today = datetime.now().strftime("%Y-%m-%d")
    file_basename = os.path.basename(local_filename)
    return os.path.join(tablename, date_today, file_basename)


def transfer_file_s3(tablename: str,
                     local_filepath: str,
                     bucket: str) -> None:
    """
    Method to Upload File to AWS S3

    Arguments:
        tablename: Name of table in S3
        local_filepath: Path to local file
        bucket: Name of S3 bucket

    Returns:
        None
    """
    s3_key = create_s3_key(tablename,
                           local_filepath)
    s3_client.meta.client.upload_file(local_filepath,
                                      bucket,
                                      s3_key)


def main(user_file: str = "users.csv",
         s3_tablename: str = "users",
         bucket: str = "insight-hdfs",
         num_fakes: int = 500,
         download_dir: str = PATH_INSTANCE):
    """
    Main method
    """
    path_users = os.path.join(download_dir, user_file)
    with open(path_users, 'w') as user_csv:
        writer = csv.writer(user_csv)
        for username, full_name in gen_fake_users(num_fakes):
            record = (username,
                      full_name)
            writer.writerow(record)

    # transfer_file_s3(s3_tablename,
    #                  path_users,
    #                  bucket)

if __name__ == '__main__':
    main()
