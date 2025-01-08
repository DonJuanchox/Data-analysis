import etl_tools
import pathlib
import etl_logger
import logging
import polars as pl
import pandas as pd
import datetime as dt
from datetime import datetime
import uk_wrangle
import re
import uk_viz

# Define paths
path = pathlib.Path(r'C:\Users\juann\OneDrive\Documentos\GitHub\Data-analysis\UK corporate - study case\data')
companies_data = path / 'companies.parquet'
officers_owners_data = path / 'officers_and_owners.parquet'

# Get logger
console = logging.StreamHandler()
logger = etl_logger.get_logger('logger', logging.WARNING, [console])

# Columns to exclude
comps_cols_excl = [
    'next_accounts_overdue', 'confirmation_statement_overdue', 'owners', 'officers',
    'average_number_employees_during_period', 'current_assets', 'last_accounts_period_end',
    'sic_codes', 'account_type', 'company_url'
]
officers_owners_cols_excl = ['company_country', 'person_id', 'person_url']

# Load data with polars
companies_lz = pl.scan_parquet(companies_data)
officers_owners_lz = pl.scan_parquet(officers_owners_data)

# Select relevant columns
companies_cols = [col for col in companies_lz.collect_schema().names() if col not in comps_cols_excl]
officers_owners_cols = [col for col in officers_owners_lz.collect_schema().names() if col not in officers_owners_cols_excl]

# Load dataframes
companies = etl_tools.read_parquet(logger, companies_data, cols=companies_cols).with_columns([
    pl.col('date_of_cessation').fill_null(pl.lit(dt.datetime.today().date())),
    pl.col('jurisdiction').fill_null('UK establishment')
]).to_pandas()

officers_owners = etl_tools.read_parquet(logger, officers_owners_data, cols=officers_owners_cols).to_pandas()
officers_owners = officers_owners.merge(companies, on='company_number', how='inner')

# Processing companies
companies = companies.assign(
    Year=lambda df: df['incorporation_date'].apply(uk_wrangle.get_year),
    city=lambda df: df['office_address'].apply(lambda address: uk_wrangle.process_address(address, logger)),
    num_days_active=lambda df: df.apply(lambda row: uk_wrangle.days_between_dates(row, logger), axis=1)
)

# Create year brackets
bins = [0, 360, 1800, 3600, 7200, float('inf')]
labels = ['<1', '1-5y', '5-10y', '10-20y', '>20y']
companies['Years bracket'] = pd.cut(companies['num_days_active'], bins=bins, labels=labels)

# Refine city and country extraction
countries = ['England', 'United Kingdom', 'Wales', 'Ireland', 'Scotland', 'Northern Ireland']
companies.loc[companies['city'].isin(countries), 'city'] = companies.loc[companies['city'].isin(countries), 'office_address'].apply(
    lambda address: uk_wrangle.process_country(address, logger)
)

# Split active and inactive companies
active_companies = companies[companies['company_status'].isin(['Active', 'Open'])]
not_active_companies = companies[~companies['company_status'].isin(['Active', 'Open'])]

# Active Companies Charts
active_city = uk_viz.prepare_city_data(active_companies)
fig_active_city = uk_viz.create_pie_chart(active_city, "city", "size", "Active Companies by City")

active_company = active_companies.groupby('company_type', as_index=False).size()
fig_active_company = uk_viz.create_bar_chart(active_company, "company_type", "size", "Active Companies by Type")

active_years = uk_viz.prepare_years_bracket_data(active_companies)
fig_active_years = uk_viz.create_pie_chart(
    active_years, "Years bracket", "size", "Active Companies by Years Bracket",
    category_orders={"Years bracket": ["<1", "1-5y", "5-10y", "10-20y", ">20y"]}
)

# Not Active Companies Charts
not_active_city = uk_viz.prepare_city_data(not_active_companies)
fig_not_active_city = uk_viz.create_pie_chart(not_active_city, "city", "size", "Not Active Companies by City")

not_active_company = not_active_companies.groupby('company_type', as_index=False).size()
fig_not_active_company = uk_viz.create_bar_chart(not_active_company, "company_type", "size", "Not Active Companies by Type")

not_active_years = uk_viz.prepare_years_bracket_data(not_active_companies)
fig_not_active_years = uk_viz.create_pie_chart(
    not_active_years, "Years bracket", "size", "Not Active Companies by Years Bracket",
    category_orders={"Years bracket": ["<1", "1-5y", "5-10y", "10-20y", ">20y"]}
)

# Save individual charts as HTML components
active_city_html = fig_active_city.to_html(full_html=False, include_plotlyjs=False)
active_company_html = fig_active_company.to_html(full_html=False, include_plotlyjs=False)
active_years_html = fig_active_years.to_html(full_html=False, include_plotlyjs=False)

not_active_city_html = fig_not_active_city.to_html(full_html=False, include_plotlyjs=False)
not_active_company_html = fig_not_active_company.to_html(full_html=False, include_plotlyjs=False)
not_active_years_html = fig_not_active_years.to_html(full_html=False, include_plotlyjs=False)

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
        <h1 class="text-center mb-5">Company Analysis</h1>
        <h2 class="text-center mb-4">Active Companies</h2>
        <div class="chart-container">
            <h3 class="text-center">Cities overviews</h3>
            <div class="chart">{active_city_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Company Types</h3>
            <div class="chart">{active_company_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Years Bracket</h3>
            <div class="chart">{active_years_html}</div>
        </div>
        <h2 class="text-center mb-4">Not Active Companies</h2>
        <div class="chart-container">
            <h3 class="text-center">Cities overviews</h3>
            <div class="chart">{not_active_city_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Company Types</h3>
            <div class="chart">{not_active_company_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Years Bracket</h3>
            <div class="chart">{not_active_years_html}</div>
        </div>
    </div>
</body>
</html>
"""

# Save to HTML file
html_file = "uk_exploration.html"
with open(html_file, "w") as file:
    file.write(html_template)

print(f"The updated HTML file with all charts has been saved as: {html_file}")