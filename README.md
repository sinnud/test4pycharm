## JSON utils
This module will: based on JSON data, create JSON format mapping file. Use JSON format
mapping file to convert JSON data into CSV like data. Then we can use csv loader tool load
JSON data into database.

This module can:

- create JSON format map based on JSON data
- update map based on new JSON data
- parse data into CSV format

## Unittest for this development
The unittest can also be treated as sample of using this module.

- tests folder includes samples for test
- tests/data have encrypted JSON data files
- tests/map store generated map file along with initialization SQL table DDL
- run test from the folder this file belong to with command `python -m unittest tests.[test_file].[test_func]`

## Auto doc generation
Create PDF version documentation, store as extra source file in repository.

- The python module sphinx to `make latex`
- The MikTeX software to `pdflatex jsonutils.tex`
- copy the PDF file to repository