# Conversion Impact Data Pull

### Summary

Pull conversion impact data from Redshift and merge into CSV for reporting.

### Requirements

You need the following installed on your machine:

0. Python 2.7
1. Database drivers:  `postgresql`
  * Tips:
    * Make sure Homebrew is installed. If it's not, Google the command to run to install it.
      * As of 23 Oct 2015, the command is: `ruby -e “$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)”`
    * Run: `brew install postgresql`
    * Run: `brew install openssl`
    * Run: `brew link --force openssl`
2. Python modules:  `pandas` for data manipulation, and `psycopg2` for PostgresSQL (Redshift) access.
  * Tips:
    * Run: `pip install pandas`
    * Run: `pip install psycopg2`
3. Store your Redshift credentials in environment variables.
  * Tips:
    * Run: `vi ~/.bash_profile`
    * Press the `i` key to use "insert" mode in vi.
    * Add the following lines to your bash profile:
      * `export MAGPIE_REDSHIFT_USER='yourusernamehere'`
      * `export MAGPIE_REDSHIFT_PWD='yourpasswordhere'`
    * Get out of insert mode by pressing `Esc`.
    * Save the file by pressing `Shift-ZZ` (Yes, two Zs)
    * Run: `source ~/.bash_profile`

### Instructions

0. Make sure the `read_queries()` function in `daily_pull.py` is pointing to the appropriate queries.
  * If request is for **Curations**:  Edit the function to open the file `curations_totals.sql`
  * If request is for **Question & Answer**:  Edit the function to open the file `qa_totals.sql`
1. Run: `python daily_pull.py`
2. Provide input for each prompt. 
  * Enter the **Workbench name** for the desired client. Use all *lowercase*. e.g. `jcpenney`
  * Enter the desired start date and end date with the format **YYYY-MM-DD**. For example:
    * Start date:  `2016-09-01`
    * End date:  `2016-09-30`
  * Both dates are inclusive. So, this would run a report for all data between September 1 and September 30. Including September 30.

