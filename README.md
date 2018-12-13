# JobPath evaluation (DEASP, OECD 2018)

## Files
### jp_outcomes.zip
- Pre-prepared pseudonymised dataset
- CSV format, zipped to save space
- Includes JobPath treatment information, plus employment earnings and SW payment information
- Q1 2016 only, for now [need to add other quarters' data]
- [Need to add monthly JobPath eligibility status]
- [Need to think about what other information to add that would be useful for analysis!]

### jobpath_evaluation.py (and .ipynb)
- [Need to break up into several smaller files!]
- Finishes tidying data [move to data preparation step]
- Implements weighting algorithm [need to create functions to encapsulate logic]
- [missing] weighting algorithm evaluation code - which algorithm(s) work best?
- [missing] recursive weighting code for all periods selected
- Creates weighted outcomes based on weighting algorithm [need to do this differently depending on selected outcome]
- Produces tables and graphs describing outputs [need to tidy code, add CIs, fix styling]

## WeeklyStatus.ipynb
 - ingests the zipped csv WeeklyStatus.csv
 - sorts labels chronologically
 - assigns weekly status values to one of five categories 
      • those in employment (0 or 1)
      •	those in receipt of weekly labour force Social Welfare Payments (0 or 1)
      •	those in receipt of weekly Social Welfare Payments (0 or 1)
      •	those taking part in education or training (0 or 1)
      •	those in receipt of Other social welfare payments (Child Benefit, Household Benefit Package, etc.) (0 or 1)
- calculates share of all weeks in each of these categories for T and C

### /images
- Save folder for graphical outputs of analysis code
- Need to fix and standardise graph formatting (see KW work)
