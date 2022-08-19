# bmo-scraper
### Getting Started
First, install Python 3.9.  Any distribution is fine (official, homebrew, conda, etc.).

From this folder, run the following commands:
```sh
$ pip install virtualenv
$ virtualenv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
```

### Usage
#### Scraping script:
In the poc folder, either run the BmoScraper.py script as an executable, or interactively run BmoScraper.ipynb to view the results step by step.

#### Get OAuth token and post new product
In the lambdas_write_to_pdw/lambda_function.py file, changde client id and client secret. 
In the lambdas_write_to_pdw/product.json file, changde the product isin so you won't get duplicates error. 
Run lambdas_write_to_pdw/lambda_function.py file


### To Do
BMO Examples.xlsx is included in this repo.  The performance of the BmoScraper object could be improved by just iterating once per note.  Further testing with additional notes is needed, and exception handling may need to be built in as development continues.
