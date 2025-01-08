import pathlib
import polars as pl
import pandas as pd
import etl_tools
import etl_logger
import logging
import matplotlib.pyplot as plt
import re
import uk_viz
from countries import countries

"""
---- Start ----
"""

# Get logger
console = logging.StreamHandler()
logger = etl_logger.get_logger('logger', logging.WARNING, [console])

path = pathlib.Path(
    r'C:\Users\juann\OneDrive\Documentos\GitHub\Data-analysis\UK corporate - study case\data')
officers_owners_data = path / 'officers_and_owners.parquet'

officers_owners_cols_excl = ['company_country', 'person_id', 'person_url']
officers_owners_lz = pl.scan_parquet(officers_owners_data)

officers_owners_cols = [col for col in officers_owners_lz.collect_schema(
).names() if col not in officers_owners_cols_excl]
officers_owners = etl_tools.read_parquet(
    logger, officers_owners_data, cols=officers_owners_cols).to_pandas()
# officers_owners = officers_owners.merge(companies, on='company_number', how='inner')
officers_owners.loc[officers_owners['country_of_residence'].isin(
    ['England', 'Scotland', 'Wales', 'Northern Ireland', 'Gbr', 'Britain']), 'country_of_residence'] = 'United Kingdom'


"""
Examine this part
"""
# Flatten the countries dictionary
flattened_countries = {}
for country, values in countries.items():
    if isinstance(values, list):
        for val in values:
            flattened_countries[val.lower()] = country  # Normalize case
    else:
        flattened_countries[values.lower()] = country

# Compile the regex pattern once
pattern = re.compile(r'[+;/,()&-]')

# Optimized function

def split_nationality_optimized(row):
    # Split on delimiters and take the first part
    # Normalize case and strip spaces
    normalized_value = pattern.split(row)[0].strip().lower()
    # Return the matching country or None
    return flattened_countries.get(normalized_value, None)
# Apply the function using vectorized operations
officers_owners['nationality'] = officers_owners['nationality'].map(
    split_nationality_optimized)
"""
"""

# Get Officer roles
officer_roles_df = officers_owners.groupby(
    'officer_role', as_index=False).size()
officer_roles_df.loc[officer_roles_df['officer_role']
                     == '', 'officer_role'] = 'Unknown'
officer_roles_df.sort_values('size', ascending=False, inplace=True)

# Get occupations
occupations_df = officers_owners.groupby('occupation', as_index=False).size().query(
    "size > 5 and occupation != ''").sort_values('size', ascending=False).reset_index(drop=True)
occupations_df.loc[occupations_df['occupation']
                   == 'none', 'occupation'] = 'Unknown'
occupations_df.occupation = occupations_df.occupation.str.title()
occupations_df = uk_viz.label_top_rows(occupations_df, 'occupation', top_n=10).groupby(
    'occupation', as_index=False).agg({'size': 'sum'})

# Owners overview
owners_df = officers_owners.groupby('is_owner', as_index=False).size()

owners_officer_roles = officers_owners.query("is_owner == True")\
    .groupby('officer_role', as_index=False).size().sort_values('size', ascending=False)

# -- Nationality distribution
nationality_df = officers_owners.groupby('nationality', as_index=False).size()
nationality_df = uk_viz.label_top_rows(nationality_df, 'nationality', top_n=20).groupby(
    'nationality', as_index=False).agg({'size': 'sum'})
nationality_excl_uk = nationality_df.query("nationality !='United Kingdom'")

# Residence vs nationality
country_residence_df = officers_owners.groupby(['country_of_residence', 'nationality'], as_index=False).size()\
    .loc[lambda x: (x.country_of_residence != '')]\
    .loc[lambda x: x.nationality != '']\
    .query('size > 1000')\
    .sort_values('size', ascending=False)\
    .reset_index(drop=True)

# Residence UK but not uk nationality
uk_residence_df = country_residence_df.query(
    "nationality != 'United Kingdom' and country_of_residence == 'United Kingdom'").reset_index(drop=True)
# UK nationality living abroad
uk_nationality = country_residence_df.query(
    "nationality == 'United Kingdom' and country_of_residence != 'United Kingdom'").reset_index(drop=True)

# Distribution owner and nationality
owners_nationality = officers_owners.query("is_owner == True").groupby(
    'nationality', as_index=False).size().sort_values('size', ascending=False)
owners_nationality = uk_viz.label_top_rows(owners_nationality, 'nationality', top_n=10).groupby(
    'nationality', as_index=False).agg({'size': 'sum'})

owners_nationality_excl_uk = officers_owners.query("is_owner == True and nationality != 'United Kingdom'").groupby(
    'nationality', as_index=False).size().sort_values('size', ascending=False)
owners_nationality_excl_uk = uk_viz.label_top_rows(owners_nationality_excl_uk, 'nationality', top_n=20).groupby(
    'nationality', as_index=False).agg({'size': 'sum'})

non_owners_nationality = officers_owners.query("is_owner == False").groupby(
    'nationality', as_index=False).size().sort_values('size', ascending=False)
non_owners_nationality = uk_viz.label_top_rows(owners_nationality, 'nationality', top_n=10).groupby(
    'nationality', as_index=False).agg({'size': 'sum'})

non_owners_nationality_excl_uk = officers_owners.query("is_owner == False and nationality != 'United Kingdom'").groupby(
    'nationality', as_index=False).size().sort_values('size', ascending=False)
non_owners_nationality_excl_uk = uk_viz.label_top_rows(owners_nationality_excl_uk, 'nationality', top_n=20).groupby(
    'nationality', as_index=False).agg({'size': 'sum'})

# Occupation for Owners whose nationality is UK and live in the UK
uk_owners_and_residents_ocuppation = officers_owners.query("is_owner == True and nationality == 'United Kingdom' and country_of_residence == 'United Kingdom'")\
    .groupby('occupation', as_index=False).size().sort_values('size', ascending=False).head(50)
uk_owners_and_residents_ocuppation = uk_viz.label_top_rows(
    uk_owners_and_residents_ocuppation, 'occupation', top_n=10).groupby('occupation', as_index=False).agg({'size': 'sum'})

# Declare visualization
officer_roles_viz = uk_viz.create_bar_chart(
    officer_roles_df, 'officer_role', 'size', 'Officer Roles Overview')
occupation_viz = uk_viz.create_pie_chart(
    occupations_df, "occupation", "size", "Occupation Overview")
owners_viz = uk_viz.create_toggleable_pie_charts([owners_df, owners_officer_roles],
                                          ['is_owner', 'officer_role'],
                                          ['size', 'size'],
                                          ['Is Owner', 'Owner Ofiicer Role'])
nationality_viz = uk_viz.create_toggleable_bar_charts(
    [nationality_df, nationality_excl_uk],
    ['nationality', 'nationality'],
    ['size', 'size'],
    ['Nationality Overview', 'Nationality Overview (excl UK)']
)

residence_nationality_viz = uk_viz.create_toggleable_bar_charts(
    [uk_residence_df, uk_nationality],
    ['nationality', 'country_of_residence'],
    ['size', 'size'],
    ['UK Residence (excl British)', 'UK Nationality (not UK resident)']
)

owners_and_non_owners_viz = uk_viz.create_toggleable_pie_charts(
    [owners_nationality, owners_nationality_excl_uk,
        non_owners_nationality, non_owners_nationality_excl_uk],
    ['nationality', 'nationality', 'nationality', 'nationality'],
    ['size', 'size', 'size', 'size'],
    ['Owners Nationality',
        'Owners Nationality (Excl UK)', 'Non Owners Nationality', 'Non Owners Nationality (Excl UK)']
)
uk_owners_and_residents_ocuppation_viz = uk_viz.create_pie_chart(
    uk_owners_and_residents_ocuppation, "occupation", "size", "Occupation for UK nationals who reside in the UK")
# Declare HTML code
officer_roles_html = officer_roles_viz.to_html(
    full_html=False, include_plotlyjs=False)
occupation_html = occupation_viz.to_html(
    full_html=False, include_plotlyjs=False)
owners_html = owners_viz.to_html(full_html=False, include_plotlyjs=False)
nationality_html = nationality_viz.to_html(
    full_html=False, include_plotlyjs=False)
residence_nationality_html = residence_nationality_viz.to_html(
    full_html=False, include_plotlyjs=False)
owners_and_non_owners_html = owners_and_non_owners_viz.to_html(
    full_html=False, include_plotlyjs=False)
uk_owners_and_residents_ocuppation_html = uk_owners_and_residents_ocuppation_viz.to_html(
    full_html=False, include_plotlyjs=False)

# HTML Template
html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Company Analysis</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{
            padding: 20px;
        }}
        .chart-container {{
            margin-bottom: 40px;
        }}
        .chart-container h3 {{
            margin-bottom: 20px;
        }}
        .chart {{
            width: 100%;
            max-width: 1000px;
            margin: 0 auto;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1 class="text-center mb-5">Officer Analysis</h1>
        <div class="chart-container">
            <h3 class="text-center">Officer Roles</h3>
            <div class="chart">{officer_roles_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Occupation</h3>
            <div class="chart">{occupation_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Owners Overview</h3>
            <div class="chart">{owners_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Nationality Overview</h3>
            <div class="chart">{nationality_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Residence and Nationality Overview</h3>
            <div class="chart">{residence_nationality_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Owners and NonOwners Overview</h3>
            <div class="chart">{owners_and_non_owners_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Owners and Residents Overview</h3>
            <div class="chart">{uk_owners_and_residents_ocuppation_html}</div>
        </div>
    </div>
</body>
</html>
"""

# Save to HTML file
html_file = "uk_roles.html"
with open(html_file, "w") as file:
    file.write(html_template)

print(f"The updated HTML file with all charts has been saved as: {html_file}")
