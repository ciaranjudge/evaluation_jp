

# //TODO 1. Define shape of input dataset and test it

# *Inputs:
# - person-IDed treatment period df with standard columns
# - [earnings] by person by year
# - [SW payments] by oerson by scheme by quarter

# *Output is dataframe with extra earnings and SW payments columns:
# - age as of this treatment period
# - sex
# - last 3 complete years of earnings as 3 columns
# - age at time of first entry into insurable employment (if any)
# - 

# ? What can we assert about output ?



# //TODO 2. Generate probability of treatment scores
# *Input is 'treatment period ++' output from (1)

# *Output is original 'treatment period ++' with 1 new column:
# - probability score

# ? What can we assert about output ?
# - Probability scores should all be 0 <= prob <= 1
# - Median and mean p-scores should be greater for T than C


# //TODO 3. Apply IPW to probability scores

# *Input is 'treatment_period ++' with probability column from (2) 

# *Output is 'treatment_period ++' with IPW column

# ? What can we assert about output from IPW ?
# - All T should have weight 1
# ? If #C > #T, all C should have weight 0 < weight < 1
# ? 3. If #C < #T, all C should have weight > 1
# - Sum of T should equal sum of C within small tolerance
# - KDE / summary statistics show smaller T vs C differences after weighting:
#   - claim duration
#   - 3-year previous earnings
#   - age

