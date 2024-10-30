import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib as mp
import matplotlib.pyplot as plt
import glob
import os
import datetime
import string
import logging
import sqlite3
from datetime import datetime, date 
import ast
import numpy as np
from sqlalchemy import create_engine
import logging
import unittest
pd.options.mode.chained_assignment = None

logging.basicConfig(filename="./dev/cleanse_db.log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    filemode='w',
                    level=logging.DEBUG,
                    force=True)
logger = logging.getLogger(__name__)
os.chdir('/Users/njaro/Downloads/subscriber-pipeline-starter-kit/subscriber-pipeline-starter-kit/dev')
def clean_student_table(students):
    con = sqlite3.connect('Cademycode.db')
    cur = con.cursor()
    table_list = [a for a in cur.execute("select name from sqlite_master where type == 'table'")]

    students = pd.read_sql_query('select * from cademycode_students', con)
    courses = pd.read_sql_query('select * from cademycode_courses', con)
    student_job = pd.read_sql_query('select * from cademycode_student_jobs', con)
    update_students = pd.read_sql_query('select * from cademycode_student_jobs', con)
    students['contact_info'] = students['contact_info'].apply(lambda x: ast.literal_eval(x))
    new_contact = pd.json_normalize(students['contact_info'])
    students = pd.concat([students.drop('contact_info', axis=1), new_contact], axis=1)
    split_address = students.mailing_address.str.split(',', expand=True)
    split_address.columns = ['street address', 'city', 'state', 'zip code']
    students =  pd.concat([students.drop('mailing_address', axis = 1), split_address], axis=1)
    students_dob = students['dob']
    students_dob_df = pd.DataFrame(data = students_dob)
    def age(born):
    
    
        born = datetime.strptime(born, "%Y-%m-%d").date()
        today = date.today() 
        return today.year - born.year - ((today.month,  
                                      today.day) < (born.month,  
                                                    born.day)) 
        students_dob_df['Age'] = students_dob_df['dob'].apply(age) 
    update_students = pd.concat([students, students_dob_df], axis=1)
    update_students['job_id'] = update_students['job_id'].astype(float)
    update_students['current_career_path_id'] = update_students['current_career_path_id'].astype(float)
    update_students['num_course_taken'] = update_students['num_course_taken'].astype(float)
    update_students['time_spent_hrs'] = update_students['time_spent_hrs'].astype(float)
    update_students['Age'] = update_students['Age'].astype(float)
    missing_course_taken = students[students[['num_course_taken']].isnull().any(axis=1)]
    missing_data = pd.DataFrame()
    missing_data = pd.concat([missing_data, missing_course_taken])
    update_students = update_students.dropna(subset=['num_course_taken'])
    missing_job_id = update_students[update_students[['job_id']].isnull().any(axis=1)]
    update_students = update_students.dropna(subset=['job_id'])
    return(update_students, missing_data)
def clean_careers(df): 
    not_applicable = {'career_path_id': 0,
                      'career_path_name': 'not applicable',
                      'hours_to_complete': 0}
    df.loc[len(df)] = not_applicable
    return(df.drop_duplicates())
def test_path(update_studnts, career_path):
    student_table = update_students.current_career_path_id.unique()
    is_subset = np.isin(student_table, career_paths.career_path_id.unique())
    missing_id = student_table[~is_subset]
    try:
        assert len(missing_id) == 0, "Missing career_path_id(s): " + str(list(missing_id)) + " in `career_paths` table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All career_path_ids are present.')
def test_for_job_id(update_students, student_jobs):
    student_table = update_students.job_id.unique()
    is_subset = np.isin(student_table, student_jobs.job_id.unique())
    missing_id = student_table[~is_subset]
    try:
        assert len(missing_id) == 0, "Missing job_id(s): " + str(list(missing_id)) + " in `student_jobs` table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('All job_ids are present.')
def test_nulls(df):
    df_missing = df[df.isnull().any(axis=1)]
    cnt_missing =len(df_missing)

    try:
        assert cnt_missing == 0, "There are " + str(cnt_missing) + " nulls in the table"
    except AssertionError as ae:
        logger.exception(ae)
        raise ae
    else:
        print('No null rows found.')
def test_schema(local_df, db_df):
       errors = 0
    for col in db_df:
        try:
            if local_df[col].dtypes != db_df[col].dtypes:
                errors+=1
        except NameError as ne:
            logger.exception(ne)
            raise ne

    if errors > 0:
        assert_err_msg = str(errors) + " column(s) dtypes aren't the same"
        logger.exception(assert_err_msg)
    assert errors == 0, assert_err_msg
def main():

    # initialize log
    logger.info("Start Log")

    # check for current version and calculate next version for changelog
    with open('./dev/changelog.md') as f:
        lines = f.readlines()
    next_ver = int(lines[0].split('.')[2][0])+1

    # connect to the dev database and read in the three tables
    con = sqlite3.connect('./dev/cademycode.db')
    students = pd.read_sql_query("SELECT * FROM cademycode_students", con)
    career_paths = pd.read_sql_query("SELECT * FROM cademycode_courses", con)
    student_jobs = pd.read_sql_query("SELECT * FROM cademycode_student_jobs", con)
    con.close()

    # get the current production tables, if they exist
    try:
        con = sqlite3.connect('./prod/cademycode_cleansed.db')
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", con)
        missing_db = pd.read_sql_query("SELECT * FROM incomplete_data", con)
        con.close()

        # filter for students that don't exist in the cleansed database
        new_students = students[~np.isin(students.uuid.unique(), clean_db.uuid.unique())]
    except:
        new_students = students
        clean_db = []

    # run the cleanse_student_table() function on the new students only
    clean_new_students, missing_data = cleanse_student_table(new_students)

    try:
        # filter for incomplete rows that don't exist in the missing data table
        new_missing_data = missing_data[~np.isin(missing_data.uuid.unique(), missing_db.uuid.unique())]
    except:
        new_missing_data = missing_data

    # upsert new incomplete data if there are any
    if len(new_missing_data) > 0:
        sqlite_connection = sqlite3.connect('./dev/cademycode_cleansed.db')
        missing_data.to_sql('incomplete_data', sqlite_connection, if_exists='append', index=False)
        sqlite_connection.close()

    # proceed only if there is new student data
    if len(clean_new_students) > 0:
        # clean the rest of the tables
        clean_career_paths = cleanse_career_path(career_paths)
        clean_student_jobs = cleanse_student_jobs(student_jobs)

        ##### UNIT TESTING BEFORE JOINING #####
        # Ensure that all required join keys are present
        test_for_job_id(clean_new_students, clean_student_jobs)
        test_for_path_id(clean_new_students, clean_career_paths)
        #######################################

        clean_new_students['job_id'] = clean_new_students['job_id'].astype(int)
        clean_new_students['current_career_path_id'] = clean_new_students['current_career_path_id'].astype(int)

        df_clean = clean_new_students.merge(clean_career_paths, left_on='current_career_path_id', right_on='career_path_id', how='left')
        df_clean = df_clean.merge(clean_student_jobs, on='job_id', how='left')

        ##### UNIT TESTING #####
        # Ensure correct schema and complete data before upserting to database
        if len(clean_db) > 0:
            test_num_cols(df_clean, clean_db)
            test_schema(df_clean, clean_db)
        test_nulls(df_clean)
        ########################

        # Upsert new cleaned data to cademycode_cleansed.db
        con = create_engine('sqlite:///./dev/cademycode_cleansed.db', echo=True)
        sqlite_connection = con.connect()
        df_clean.to_sql('cademycode_aggregated', sqlite_connection, if_exists='append', index=False)
        clean_db = pd.read_sql_query("SELECT * FROM cademycode_aggregated", con)
        sqlite_connection.close()

        # Write new cleaned data to a csv file
        clean_db.to_csv('./dev/cademycode_cleansed.csv')

        # create new automatic changelog entry
        new_lines = [
            '## 0.0.' + str(next_ver) + '\n' +
            '### Added\n' +
            '- ' + str(len(df_clean)) + ' more data to database of raw data\n' +
            '- ' + str(len(new_missing_data)) + ' new missing data to incomplete_data table\n' +
            '\n'
        ]
        w_lines = ''.join(new_lines + lines)

        # update the changelog
        with open('./dev/changelog.md', 'w') as f:
            for line in w_lines:
                f.write(line)
    else:
        print("No new data")
        logger.info("No new data")
    logger.info("End Log")


if __name__ == "__main__":
    main()