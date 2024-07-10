# Overview

I applied for a Data Engineer position and had an initial interview followed by a take home task with a follow-up technical interview about said task.

I spent over 4 hours on the task but after submitting the task there was no follow through on the company's end until I followed up over a week later. 

After the technical interview, I received no feedback or outcome. I was ghosted.

This article seems applicable - https://www.forbes.com/sites/jackkelly/2024/03/01/job-ghosting

# Company

Boutique consultancy - Edgered

# Technical Task and Interview

## Position

Data Engineer

### Job Description:

We are seeking a talented and motivated Data Engineer to join our team and contribute to our exciting projects at the intersection of technology and data. As a Data Engineer, you will work closely with our experienced professionals, utilising your technical skills in Python and SQL to build robust data pipelines, implement data models, and enable data-driven decision-making for our clients. This is an excellent opportunity to gain hands-on experience, grow your skills, and make a real impact in the industry.

### Responsibilities:

- Collaborate with cross-functional teams to understand data requirements and develop scalable data solutions.
- Design and implement data pipelines, ensuring data quality, reliability, and performance.
- Develop and maintain data models, ensuring consistency and accessibility of data for analysis.
- Build and optimize data storage and retrieval systems on cloud platforms such as AWS/Azure.
- Perform data cleansing, transformation, and integration tasks to ensure data accuracy and integrity.
- Utilize AWS/Azure services to automate and streamline data processes.
- Work with automation tools that support the cloud 
- Monitor and troubleshoot data pipelines, identifying and resolving issues promptly.
- Stay updated with industry trends and emerging technologies in data engineering.

# Requirements and task provided by interviewer
 
You have been provided with the following files:
- a sqlite database | edgered.db
- sql script with table definitions | create_tables.sql
- sample output which you will need to replicate | sample-output.json

Your task is to write a program that achieves the following:
- Extracts the data out of the sqlite database.
- Transforms the data into the desired form as shown in sample-output.json but on the entire dataset.  The final output must be exactly the same format. 
- Writes the data into a json file.

You are required to use python but are free to use any other packages and/or tools you wish. You will also have to calculate each of the summary fields in sample-output.json

Additionally:
- The path to the database, and the path to the output file must be configured via command line argument.
- The payment_amt in the payments table is in cents.
- The transaction_date in the payments table is in epoch timestamp.

Please return the completed case study within 7 days.  Let me know if any issues arise. 

# Solution Usage

Version runs on Python 3.10.11 and tested on 3.11.3

Run:

pip install -r requirements.txt

python main.py {path to SQLite database} {output filename}

{path to SQLite database} required beginning the file path with / for relative path or // absolute path
{output filename}

Example:

python main.py /edgered.db test_output.json

This will retrieve the data from the file in the same directory (relative) 
called edgered.db, transform to the required output, and write to 
test_output.json file.

# Questions from interviewer about solution

1. Is it secure?
2. Why does the solution not prevent SQL injection?
3. Show me you can write a for loop.
4. Why did I use SQLAlchemy and not just purely pandas?
5. Do you know what Hive is?

The context of the interviewers questions in the context of the technical environments in the job description and technical task were not aligned. The approach of the interviewer was -not- to broaden the context of this task and using it as a transformation how would go able including it in a pipeline and as part of the orchestration. Instead the approach was to fixate on minutiae, 'What does this line of your code do', rather than understanding a process of thought towards building our an ETL/ELT in a cloud environment as the job description suggests.

The question about SQLAlchemy is understandable in isolation but in context of the job description - Python & SQL - being a core skill emphasised then it seemed appropriate to include some SQL in the solution.
