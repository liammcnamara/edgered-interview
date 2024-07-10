"""
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

"""

# Install dependencies
import subprocess

subprocess.run('pip3 install locale datetime json argparse jsonschema sqlalchemy numpyencoder pandas')

# Import libraries required
import locale, datetime, json, argparse, jsonschema, sqlalchemy, numpyencoder, pandas


""" dbTransformationFunctions class
    Contains all the helper and other functions required to transform the
    database and create the required output.
"""

class dbTransformationFunctions:

    """ Initialise transformer

        db_file_path - path to SQLite database file
    """
    def __init__(self, db_file_path):
        db_path_prefix = 'sqlite://'
        self.dbEngine=sqlalchemy.create_engine(db_path_prefix+db_file_path)
    

    """ execQuery Helper Function
        Execute any query on the SQLite database

        query - accepts SQL query
    """
    def __execQuery(self, query):
       return pandas.read_sql(query,self.dbEngine)
    

    """ convertToCurreny Helper Function
        Converts integer value to standardised curreny format

        value - integer value in cents
    """
    def __convertToCurrency(self, value):
        locale.setlocale(locale.LC_ALL, '')
        return locale.currency(int(value)/100,grouping=True)


    """ convertToDatetime Helper Function
        Converts epoch to datetime

        value - epoch value
        format - defaults to standard format required for schema. Accepts any valid format string.
    """
    def __convertToDatetime(self, value, format='%Y-%m-%dT%H:%M:%S'):
        return datetime.datetime.fromtimestamp(value).strftime(format)
    

    """ getPaymentTypes Function
        Returns distinct payment codes used in payments table
    """
    def getPaymentTypes(self):
        query = f"""
                    SELECT DISTINCT payment_code
                    FROM payments
                """
        return self.__execQuery(query)


    """ getClientPaymentRecords Function
        Returns payment records for a specific client_id

        client_id - client id from clients table
    """
    def getClientPaymentRecords(self, client_id):
        query = f"""
                    SELECT 
                        transaction_id,
                        contract_id,
                        transaction_date,
                        payment_amt,
                        payment_code
                    FROM payments
                    WHERE client_id = {client_id}
                """
        return self.__execQuery(query)
    

    """ getClientIds Function
        Returns client ids from client table
    """
    def getClientIds(self):
        query = f"""
                    SELECT DISTINCT client_id
                    FROM clients
                """
        return self.__execQuery(query)
    

    """ getClientDetails Function
        Returns client aggregate details and payment history

        client_id - Specific client id to retrieve data for

        This function will help to complete this part of the output required.

            "client_id": 1279,
            "entity_type": "Discretionary Trading Trust",
            "entity_year_established": 2017,
            "total_payments": 4,
            "total_amt_paid": "$1,086,583.20",

    """
    def getClientDetails(self, client_id):
        query = f"""
                    WITH client_payment_aggr AS (
                        SELECT 
                            client_id,
                            COUNT(transaction_id) AS total_payments,
                            SUM(payment_amt) AS total_amt_paid
                        FROM payments AS pay
                        GROUP BY client_id
                    )

                    SELECT 
                        cli.client_id,
                        cli.entity_type,
                        cli.entity_year_established,
                        cpa.total_payments,
                        cpa.total_amt_paid
                    FROM clients AS cli
                    LEFT JOIN client_payment_aggr AS cpa ON cpa.client_id = cli.client_id
                    WHERE cli.client_id = {client_id}
                """
        return self.__execQuery(query)
    

    """ getSummary Function
        Return aggregate summary data

        This function will create the following section required in the output:

            "summary": {
                "total_clients": 1234,
                "total_payments": 1234,
                "oldest_payment": "2023-01-01T11:11:11",
                "newest_payment": "2023-01-01T11:11:11",
                "sum_all_payments": "$1,000.00",
                "average_payment": "$1,000.00",
                "payment_min": "$1,000.00",
                "payment_quartile_1": "$1,000.00",
                "payment_median": "$1,000.00",
                "payment_quartile_3": "$1,000.00",
                "payment_max": "$1,000.00",
                "total_amt_paid_in_june_and_july": "$1,000.00",
                "total_private_companies": 1234,
                "total_num_payments_under_1_dollar": 1234,
                "average_sole_trader_payment_in_2017": "$1,000.00"
            },
    """
    def getSummary(self):

        # Summary query does not include DEFAULT payment code
        query = f"""
                    SELECT 
                        pay.*,
                        cli.entity_type AS entity_type
                    FROM payments AS pay
                    LEFT JOIN clients AS cli ON cli.client_id = pay.client_id
                    WHERE pay.payment_code = "PAYMENT"
                """
        df = self.__execQuery(query)

        # Add columns for month and year of transaction date
        df['transformed_tr_month'] = df['transaction_date'].apply(lambda x: int(self.__convertToDatetime(x, '%m')))
        df['transformed_tr_year'] = df['transaction_date'].apply(lambda x: int(self.__convertToDatetime(x, '%Y')))
        
        # Construct summary using pandas dataframe
        summary = {
            "total_clients": df['client_id'].nunique(),
            "total_payments": df['transaction_id'].nunique(),
            "oldest_payment": self.__convertToDatetime(df['transaction_date'].agg('min')),
            "newest_payment": self.__convertToDatetime(df['transaction_date'].agg('max')),
            "sum_all_payments": self.__convertToCurrency(df['payment_amt'].agg('sum')),
            "average_payment": self.__convertToCurrency(df['payment_amt'].agg('mean')),
            "payment_min": self.__convertToCurrency(df['payment_amt'].agg('min')),
            "payment_quartile_1": self.__convertToCurrency(df['payment_amt'].quantile(0.25)),
            "payment_median": self.__convertToCurrency(df['payment_amt'].agg('median')),
            "payment_quartile_3": self.__convertToCurrency(df['payment_amt'].quantile(0.75)),
            "payment_max": self.__convertToCurrency(df['payment_amt'].agg('max')),
            "total_amt_paid_in_june_and_july": self.__convertToCurrency(df.query('transformed_tr_month == 5 or transformed_tr_month == 6')['payment_amt'].agg('sum')),
            "total_private_companies": df.query('entity_type == "Australian Private Company"')['client_id'].nunique(),
            "total_num_payments_under_1_dollar": df.query('payment_amt < 100')['transaction_id'].agg('count'),
            "average_sole_trader_payment_in_2017": self.__convertToCurrency(df.query('transformed_tr_year == 2017')['payment_amt'].agg('mean'))
        }

        return summary
    

    """ getClientRecords Function
        Returns records object

        This function will create the following part of the required output:

        "records": [
            {
                "client_id": 1279,
                "entity_type": "Discretionary Trading Trust",
                "entity_year_established": 2017,
                "total_payments": 4,
                "total_amt_paid": "$1,086,583.20",
                "payments": [
                    {
                        "transaction_id": 24808,
                        "contract_id": 1608,
                        "transaction_date": "2018-07-18T04:09:21",
                        "payment_amt": "$999,999.70",
                        "payment_code": "PAYMENT"
                    },
                    ....
                ]
            }
        ......
        ]

    """
    def getClientRecords(self):
        records = []
        cli_ids = self.getClientIds()

        for cli_id in cli_ids.iterrows():
            client_details = self.getClientDetails(cli_id[0])
            client_details_dict = client_details.to_dict(orient='records')
            if client_details_dict != []: 
                payments = self.getClientPaymentRecords(cli_id[0])
                payments['transaction_date'] = payments['transaction_date'].apply(lambda x: self.__convertToDatetime(x))
                payments['payment_amt'] = payments['payment_amt'].apply(lambda x: self.__convertToCurrency(x))
                client_details['total_amt_paid'] = client_details['total_amt_paid'].apply(lambda x: self.__convertToCurrency(x))
                client_details_dict = client_details.to_dict(orient='records')[0]
                client_details_dict["payments"] = payments.to_dict(orient='records')
                records.append(client_details_dict)
        
        return records


    """ generateJSON Function
        Returns JSON Object of sumnmary and records data
    """
    def generateJSON(self):
        output = {
            "summary": self.getSummary(),
            "records": self.getClientRecords()
        }
        output = json.dumps(output, cls=numpyencoder.NumpyEncoder)
        return json.loads(output)


""" main Function
    Executes the transformation and export from the database based on input arguments.

    Arguments required:
    {path to SQLite database} - e.g. 'edgered.db'
    {output filename} - e.g. 'test-output.json'

"""

def main():
    
    try:
        
        # Create command-line argument parser
        parser = argparse.ArgumentParser(description='This is a new command-line tool')
        parser.add_argument('database_file', help='Path to the input file')
        parser.add_argument('output_file', help='Path to the input file')
        args = parser.parse_args()

        print(f'Input file: {args.database_file}')

        # Generate JSON output from transformation
        transformer = dbTransformationFunctions(db_file_path=args.database_file)
        jsonoutput = transformer.generateJSON()

        # Import schema
        json_schema_file = open('json-schema.json')
        json_schema = json.load(json_schema_file)

        # Validate JSON output of transformation with schema
        is_valid = jsonschema.validate(instance=jsonoutput, schema=json_schema)

        if is_valid is None:
            print(f'Valid JSON Output: Yes')

            # Write JSON output to file
            with open(args.output_file, 'w', encoding='utf-8') as file:
                json.dump(jsonoutput, file, ensure_ascii=False, indent=4)
        else:
            print(f'Valid JSON Output: No')
        
        print(f'Written JSON Output to file {args.output_file}')
    except jsonschema.exceptions.ValidationError as e:
        print("this is validation error:", e)

if __name__ == "__main__":
    main()