import mysql.connector
import time
import json
import requests
from datetime import date
import html2text
from datetime import timedelta # https://stackoverflow.com/questions/10048249/how-do-i-determine-if-current-time-is-within-a-specified-range-using-pythons-da


# Connect to database
# You may need to edit the connect function based on your local settings.#I made a password for my database because it is important to do so. Also make sure MySQL server is running or it will not connect
def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='cne340')
    return conn


# Create the table structure
def create_tables(cursor):
    # Creates table
    # Must set Title to CHARSET utf8 unicode Source: http://mysql.rjweb.org/doc.php/charcoll.
    # Python is in latin-1 and error (Incorrect string value: '\xE2\x80\xAFAbi...') will occur if Description is not in unicode format due to the json data
    query = cursor.execute(
        "CREATE TABLE IF NOT EXISTS jobs(Job_id INT PRIMARY KEY AUTO_INCREMENT, Description TEXT CHARACTER SET utf8 COLLATE utf8_unicode_ci, Created_at DATE)")  # https://stackoverflow.com/questions/766809/whats-the-difference-between-utf8-general-ci-and-utf8-unicode-ci
    return query_sql(cursor, query)


# Query the database.
# You should not need to edit anything in this function
def query_sql(cursor, query):
    cursor.execute(query)
    return cursor


# Add a new job
def add_new_job(cursor, jobdetails):
    # extract all required columns
    description = html2text.html2text(jobdetails['description'])  # This is the description of the job
    date = jobdetails['publication_date'][0:10]   # This is the date the job was posted, https://stackoverflow.com/questions/509211/understanding-slice-notation
    query = cursor.execute("INSERT INTO jobs( Description, Created_at) "  # This is the query to insert the job into the database
                "VALUES(%s,%s)", (description, date))
    # %s is what is needed for Mysqlconnector as SQLite3 uses ? the Mysqlconnector uses %s
    return query_sql(cursor, query)


# Check if new job
def check_if_job_exists(cursor, jobdetails):
    query = "SELECT * FROM jobs WHERE Job_id = " + str(jobdetails['id']) # This is the query to check if the job still exists, I was getting errors until I converted the id to a string
    return query_sql(cursor, query)

# Deletes job
def delete_job(cursor, jobdetails):
    ##Add your code here
    query = "DELETE FROM jobs WHERE Job_id = " + str(jobdetails['id'])  # This is the query to delete the job if it already exists, I was getting errors until I converted the id to a string, https://stackoverflow.com/questions/509211/understanding-slice-notation
    return query_sql(cursor, query)


# Grab new jobs from a website, Parses JSON code and inserts the data into a list of dictionaries do not need to edit
def fetch_new_jobs():
    query = requests.get("https://remotive.com/api/remote-jobs")
    datas = json.loads(query.text)['jobs']
    return datas


# Main area of the code. Should not need to edit
def jobhunt(cursor):
   # Fetch jobs from website
   jobpage = fetch_new_jobs()  # Gets API website and holds the json data in it as a list
   # use below print statement to view list in json format
   print(jobpage)
   add_or_delete_job(cursor, jobpage)


def add_or_delete_job(cursor, jobpage):
    # Add your code here to parse the job page
    for jobdetails in jobpage:  # EXTRACTS EACH JOB FROM THE JOB LIST. It errored out until I specified jobs. This is because it needs to look at the jobs dictionary from the API. https://careerkarma.com/blog/python-typeerror-int-object-is-not-iterable/
        # Add in your code here to check if the job already exists in the DB
        check_if_job_exists(cursor, jobdetails)
        is_job_found = len(
            cursor.fetchall()) > 0  # https://stackoverflow.com/questions/2511679/python-number-of-rows-affected-by-cursor-executeselect
        current_date = date.today() # Current date
        job_posted_date = jobdetails['publication_date'][0:10] #Date the job was posted
        job_posted_date = date(int(job_posted_date[0:4]), int(job_posted_date[5:7]), int(job_posted_date[8:10])) # https://stackoverflow.com/questions/509211/understanding-slice-notation and https://stackoverflow.com/questions/9987818/how-to-check-if-the-current-time-is-in-range-in-python

        if is_job_found:
            delete_job(cursor, jobdetails)
            print("Job Deleted: " + jobdetails['title'] + " at " + jobdetails['company_name'] + " on " + jobdetails['publication_date'])

        if (current_date - job_posted_date) > timedelta(days=14): #delete job if it is older than 14 days, https://stackoverflow.com/questions/10048249/how-do-i-determine-if-current-time-is-within-a-specified-range-using-pythons-da
            delete_job(cursor, jobdetails)
            print("Job Deleted: " + jobdetails['title'] + " at " + jobdetails['company_name'] + " on " + jobdetails['publication_date'])

        else:
            # INSERT JOB
            # Add in your code here to notify the user of a new posting. This code will notify the new user
            add_new_job(cursor, jobdetails)
            print("New Job Found: " + jobdetails['title'] + " at " + jobdetails['company_name'] + " on " + jobdetails['publication_date'])

    return cursor

# Setup portion of the program. Take arguments and set up the script
# You should not need to edit anything here.
def main():
   # Important, rest are supporting functions
   # Connect to SQL and get cursor
   conn = connect_to_sql()
   cursor = conn.cursor()
   create_tables(cursor)

   while 1:  # Infinite Loops. Only way to kill it is to crash or manually crash it. We did this as a background process/passive scraper
       jobhunt(cursor)
       time.sleep(864000)  # Sleep for 4h, this is ran every hour because API or web interfaces have request limits. Your request will get blocked.

# Sleep does a rough cycle count, system is not entirely accurate
# If you want to test if script works change time.sleep() to 10 seconds and delete your table in MySQL
if __name__ == '__main__':
    main()
