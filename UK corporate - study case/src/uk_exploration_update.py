"""
TODO> Clean code, rename variables, more sections, optimize code, type hints
Use replace instead of .loc
"""
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
import data_visualize as viz
from countries import world_countries


# Process companies dataframe
def process_companies_data(companies):
    """
    Processes the companies dataframe by adding new calculated columns and refining city information.

    Args:
        companies (DataFrame): The input companies dataframe.

    Returns:
        DataFrame: The processed companies dataframe.
    """
    # Add calculated columns
    companies = companies.assign(
        Year=lambda df: df['incorporation_date'].apply(uk_wrangle.get_year),
        city=lambda df: df['office_address'].apply(
            lambda address: uk_wrangle.process_address(address, logger)),
        num_days_active=lambda df: df.apply(
            lambda row: uk_wrangle.days_between_dates(row, logger), axis=1),
        Years_bracket=lambda df: pd.cut(df['num_days_active'],
                                        bins=[0, 360, 1800, 3600,
                                              7200, float('inf')],
                                        labels=['<1', '1-5y', '5-10y', '10-20y', '>20y']))
    # Refine city and country extraction
    companies.loc[companies['city'].isin(ENGLISH_COUNTRIES), 'city'] = companies.loc[companies['city'].isin(ENGLISH_COUNTRIES), 'office_address'].apply(
        lambda address: uk_wrangle.process_country(address, logger))

    return companies


# Process officers_owners dataframe
def process_officers_owners_data(officers_owners, world_countries):
    """
    Processes the officers_owners dataframe by refining country information and applying optimized functions.

    Args:
        officers_owners (DataFrame): The input officers_owners dataframe.
        world_countries (dict): Dictionary mapping normalized country names to full names.

    Returns:
        DataFrame: The processed officers_owners dataframe.
    """
    officers_owners.loc[officers_owners['country_of_residence'].isin(
        ENGLISH_COUNTRIES), 'country_of_residence'] = 'United Kingdom'

    # Apply the optimized function to the 'nationality' column
    officers_owners['nationality'] = officers_owners['nationality'].apply(
        lambda row: split_nationality_optimized(
            row, country_dict=WORLD_COUNTIES)
    )
    return officers_owners


def split_nationality_optimized(row, country_dict):
    """
    Extracts and normalizes the nationality from the input row using a precompiled regex pattern.
    Returns the corresponding country from the provided dictionary or None if not found.

    Args:
    row (str): Input string containing the nationality information.
    a_dict (dict): Dictionary mapping normalized values to countries.

    Returns:
    str or None: Matching country or None if no match is found.
    """
    # Compile the regex pattern once (compile at the top if reused globally)
    pattern = re.compile(r'[+;/,()&-]')
    # Split on delimiters, normalize case, and strip spaces
    normalized_value = pattern.split(row)[0].strip().lower()
    # Return the matching country or None
    return country_dict.get(normalized_value)


def prepare_grouped_data(data, group_by_column, top_n=None):
    """
    Groups data by a specified column, counts the occurrences, sorts by size, and optionally limits to top N results.

    Parameters:
    - data (pd.DataFrame): Input DataFrame.
    - group_by_column (str): Column to group by.
    - top_n (int, optional): Number of top results to return. If None, return all rows.

    Returns:
    - pd.DataFrame: Grouped and sorted data.
    """
    grouped_data = (
        data.groupby([group_by_column], as_index=False)
        .size()
        .sort_values('size', ascending=False)
        .reset_index(drop=True)
    )
    if top_n:
        grouped_data = grouped_data.head(top_n)
    return grouped_data


"""
Setup Environment: Define paths, initialize logger, and declare constants.

This section includes:
1. Path Definitions:
   - BASE_PATH: The base directory for data files.
   - COMPANIES_DATA_PATH: Path to the companies data file.
   - OFFICERS_OWNERS_DATA_PATH: Path to the officers and owners data file.

2. Logger Initialization:
   - Creates a logger named 'logger' with a WARNING level and console handler.

3. Constant Definitions:
   - COMPANIES_COLS_TO_EXCL: List of columns to exclude when processing companies data.
   - OFFICERS_OWNERS_COLS_TO_EXCL: List of columns to exclude when processing officers and owners data.
   - ENGLISH_COUNTRIES: List of English-speaking countries to be used for filtering or validation.
   - WORLD_COUNTIES: Flattened dictionary mapping normalized country names to their full names.
"""
# pathlib.Path(__file__).parent.resolve()
# DATA_PATH = BASE_PATH / 'data'
BASE_PATH = pathlib.Path(
    r'C:\Users\juann\OneDrive\Documentos\GitHub\Data-analysis\UK corporate - study case\data')
COMPANIES_DATA_PATH = BASE_PATH / 'companies.parquet'
OFFICERS_OWNERS_DATA_PATH = BASE_PATH / 'officers_and_owners.parquet'

# Get logger
console = logging.StreamHandler()
logger = etl_logger.get_logger('logger', logging.WARNING, [console])

# Columns to exclude
COMPANIES_COLS_TO_EXCL = ['next_accounts_overdue', 'confirmation_statement_overdue',
                          'owners', 'officers', 'average_number_employees_during_period',
                          'current_assets', 'last_accounts_period_end',
                          'sic_codes', 'account_type', 'company_url']
OFFICERS_OWNERS_COLS_TO_EXCL = ['company_country', 'person_id', 'person_url']
ENGLISH_COUNTRIES = ['England', 'United Kingdom', 'Wales', 'Ireland',
                     'Scotland', 'Northern Ireland', 'Gbr', 'Britain']
# Flatten the countries dictionary
WORLD_COUNTIES = {val.lower(): country  # Normalize case
                  for country, values in world_countries.items()
                  for val in (values if isinstance(values, list) else [values])}
"""
Load and Process Data: Explain the use of Polars and Pandas, and improve efficiency and error handling.

This section performs the following steps:
1. **Data Loading with Polars**:
   - Polars is used for its performance advantages when handling large datasets, especially for operations like schema inspection and lazy execution.
   - `pl.scan_parquet` is utilized to load metadata without immediately reading the entire dataset into memory.

2. **Column Filtering**:
   - Excludes unnecessary columns to optimize the data loading process by using predefined exclusion lists (`COMPANIES_COLS_TO_EXCL` and `OFFICERS_OWNERS_COLS_TO_EXCL`).

3. **Data Transformation**:
   - After filtering columns, the data is read into Polars DataFrames using `etl_tools.read_parquet`.
   - Columns like `date_of_cessation` and `jurisdiction` are processed to fill missing values.
   - The final data is converted to Pandas DataFrames for compatibility with downstream workflows.

4. **Potential Improvement**:
   - Loading data once into memory instead of multiple passes can improve efficiency.
   - Error handling is missing. A `try/except` block should be added to catch cases where the parquet files are missing or empty.

5. **Try/Except Error Handling**:
   - Ensures graceful failure if data files are unavailable or improperly formatted, logging the issue for debugging.

Why Polars and Pandas?
- **Polars**: Efficient for columnar data processing, schema inspection, and lazy evaluation of large datasets.
- **Pandas**: Widely compatible with existing Python workflows, providing flexibility for complex transformations and integrations.
"""
# Scan parquet files
companies = pl.scan_parquet(COMPANIES_DATA_PATH)
officers_owners = pl.scan_parquet(OFFICERS_OWNERS_DATA_PATH)

# Exclude columns
companies_cols = [col for col in companies.collect_schema().names()
                  if col not in COMPANIES_COLS_TO_EXCL]
officers_owners_cols = [col for col in officers_owners.collect_schema().names()
                        if col not in OFFICERS_OWNERS_COLS_TO_EXCL]

# Load and process companies dataframe
companies = etl_tools.read_parquet(logger, COMPANIES_DATA_PATH, cols=companies_cols)\
    .with_columns([pl.col('date_of_cessation').fill_null(pl.lit(dt.datetime.today().date())),
                   pl.col('jurisdiction').fill_null('UK establishment')])\
    .to_pandas()
# Load officers and owners dataframe
officers_owners = etl_tools.read_parquet(logger, OFFICERS_OWNERS_DATA_PATH,
                                         cols=officers_owners_cols).to_pandas()
"""
Data Wrangling: Process and split company data.

This section performs the following steps:

1. **Data Processing**:
   - Applies custom processing functions (`process_companies_data` and `process_officers_owners_data`) to clean and transform the `companies` and `officers_owners` datasets.
   - The `WORLD_COUNTIES` dictionary is passed to `process_officers_owners_data` for consistent normalization of country names.

2. **Splitting Active and Inactive Companies**:
   - Splits the `companies` dataset into `active_companies` and `not_active_companies` based on the `company_status` column.
   - Active companies are identified by statuses such as "Active" or "Open".
   - Inactive companies include all other statuses.

3. **Purpose**:
   - Processed datasets are ready for further analysis, visualization, or reporting.
   - The split ensures focused analysis on active or inactive companies as needed.

Future Improvements:
- Ensure consistency in the definition of active/inactive statuses by externalizing the list of statuses to a configuration file or constant.
- Consider handling edge cases where `company_status` values are missing or undefined.
"""
# Apply processing functions
companies = process_companies_data(companies)
officers_owners = process_officers_owners_data(officers_owners, WORLD_COUNTIES)

# Split active and inactive companies
active_companies = companies[companies['company_status'].isin(['Active', 'Open'])]
not_active_companies = companies[~companies['company_status'].isin(['Active', 'Open'])]

"""
Create DataFrame Visualizations: Generate and transform data for visualization.

This section performs the following tasks:

1. **Active and Inactive Companies**:
   - Aggregates data for active and inactive companies based on city, company type, and years bracket using the `prepare_grouped_data` function.

2. **Officer Roles**:
   - Aggregates officer roles, fills missing roles with "Unknown," and sorts the data by size.

3. **Occupations**:
   - Groups occupations, filters entries with size > 5 and non-empty occupation values, and replaces "none" with "Unknown."
   - Converts occupation names to title case, labels top 10 occupations, and aggregates the rest under "Other."

4. **Owners Overview**:
   - Aggregates data for ownership status (`is_owner`) and further groups officer roles for owners.

5. **Nationality Distribution**:
   - Groups and labels nationality data, highlighting the top 20 nationalities and aggregating the rest under "Other."
   - Segments nationality distribution for:
     - Residents in the UK but not of UK nationality.
     - UK nationals living abroad.

6. **Residence vs Nationality**:
   - Groups data by `country_of_residence` and `nationality`, filters out empty values, and includes only rows with size > 1000.
   - Extracts subsets for:
     - UK residents with non-UK nationality.
     - UK nationals residing abroad.

7. **Distribution of Owners and Nationality**:
   - Aggregates nationality distribution for owners and labels the top 10 nationalities.
   - Separates data for:
     - Owners with nationality other than the UK.
     - Non-owners grouped by nationality, segmented similarly.

8. **Occupation for UK Owners and Residents**:
   - Groups occupations for owners with UK nationality who reside in the UK.
   - Labels the top 10 occupations and aggregates the rest.

Purpose:
- Prepares grouped, filtered, and aggregated data for visualization and analysis.
- Handles missing values and labels non-top entries for better interpretability in visual outputs.

Future Improvements:
- Consolidate repetitive logic (e.g., top N labeling and grouping) into reusable helper functions.
- Parameterize thresholds (e.g., size > 5, top 10) for flexibility.
- Optimize query performance by reducing redundant sorting and filtering.
"""
# Process data for active companies
active_city = prepare_grouped_data(active_companies, 'city', top_n=50)
active_company = prepare_grouped_data(active_companies, 'company_type')
active_years = prepare_grouped_data(active_companies, 'Years_bracket')

# Process data for not active companies
not_active_city = prepare_grouped_data(not_active_companies, 'city', top_n=50)
not_active_company = prepare_grouped_data(not_active_companies, 'company_type')
not_active_years = prepare_grouped_data(not_active_companies, 'Years_bracket')

# Get Officer roles
officer_roles_df = prepare_grouped_data(officers_owners, 'officer_role')\
    .sort_values('size', ascending=False)

officer_roles_df.loc[officer_roles_df['officer_role']
                     == '', 'officer_role'] = 'Unknown'
# Get occupations
occupations_df = prepare_grouped_data(officers_owners, 'occupation')\
    .query("size > 5 and occupation != ''")\
    .sort_values('size', ascending=False)\
    .reset_index(drop=True)\
    .assign(occupation=lambda df: df['occupation'].replace('none', 'Unknown').str.title())

occupations_df = viz.label_top_rows(occupations_df, 'occupation', top_n=10)\
    .groupby('occupation', as_index=False).agg({'size': 'sum'})

# Owners overview
owners_df = prepare_grouped_data(officers_owners, 'is_owner')
owners_officer_roles = prepare_grouped_data(officers_owners.query("is_owner == True"), 'officer_role')\
    .sort_values('size', ascending=False)

# -- Nationality distribution
nationality_df = prepare_grouped_data(officers_owners, 'nationality')
nationality_df = viz.label_top_rows(nationality_df, 'nationality', top_n=20).groupby(
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
uk_residence_df = country_residence_df.query("nationality != 'United Kingdom' and country_of_residence == 'United Kingdom'")\
    .reset_index(drop=True)
# UK nationality living abroad
uk_nationality = country_residence_df.query("nationality == 'United Kingdom' and country_of_residence != 'United Kingdom'")\
    .reset_index(drop=True)

# Distribution owner and nationality
owners_nationality = prepare_grouped_data(officers_owners.query("is_owner == True"), 'nationality')\
    .sort_values('size', ascending=False)
owners_nationality = viz.label_top_rows(owners_nationality, 'nationality', top_n=10)\
    .groupby('nationality', as_index=False).agg({'size': 'sum'})

owners_nationality_excl_uk = prepare_grouped_data(officers_owners.query("is_owner == True and nationality != 'United Kingdom'"), 'nationality')\
    .sort_values('size', ascending=False)
owners_nationality_excl_uk = viz.label_top_rows(owners_nationality_excl_uk, 'nationality', top_n=20)\
    .groupby('nationality', as_index=False).agg({'size': 'sum'})


non_owners_nationality = prepare_grouped_data(officers_owners.query(
    "is_owner == False"), 'nationality').sort_values('size', ascending=False)
non_owners_nationality = viz.label_top_rows(owners_nationality, 'nationality', top_n=10)\
    .groupby('nationality', as_index=False).agg({'size': 'sum'})

non_owners_nationality_excl_uk = prepare_grouped_data(officers_owners.query(
    "is_owner == False and nationality != 'United Kingdom'"), 'nationality').sort_values('size', ascending=False).sort_values('size', ascending=False)
non_owners_nationality_excl_uk = viz.label_top_rows(owners_nationality_excl_uk, 'nationality', top_n=20)\
    .groupby('nationality', as_index=False).agg({'size': 'sum'})

# Occupation for Owners whose nationality is UK and live in the UK
uk_owners_and_residents_ocuppation = prepare_grouped_data(officers_owners.query("is_owner == True and nationality == 'United Kingdom' and country_of_residence == 'United Kingdom'"), 'occupation')\
    .sort_values('size', ascending=False).head(50)
uk_owners_and_residents_ocuppation = viz.label_top_rows(
    uk_owners_and_residents_ocuppation, 'occupation', top_n=10).groupby('occupation', as_index=False).agg({'size': 'sum'})

"""
Create Visualizations: Generate interactive and static charts for companies and officers data.

This section creates a series of visualizations to explore and summarize the processed data. The visualizations include pie charts, bar charts, and toggleable charts for comparing different subsets of data.

**Visualizations Overview**:

1. **Companies Data**:
   - `cities_viz`: Toggleable pie charts for active and not active companies by city.
   - `company_type_viz`: Toggleable bar charts for active and not active companies by type.
   - `years_viz`: Toggleable pie charts showing the distribution of active and not active companies across years brackets.

2. **Officers Data**:
   - `officer_roles_viz`: Bar chart summarizing the distribution of officer roles.
   - `occupation_viz`: Pie chart showing the top occupations with remaining grouped as "Other."
   - `owners_viz`: Toggleable pie charts for:
     - Distribution of ownership status (`is_owner`).
     - Officer roles for owners.

3. **Nationality and Residence**:
   - `nationality_viz`: Toggleable bar charts for:
     - Overall nationality distribution.
     - Nationality distribution excluding UK nationals.
   - `residence_nationality_viz`: Toggleable bar charts for:
     - Non-UK nationals residing in the UK.
     - UK nationals residing abroad.

4. **Owners and Non-Owners Nationality**:
   - `owners_and_non_owners_viz`: Toggleable pie charts showing:
     - Owners' nationality (including and excluding UK nationals).
     - Non-owners' nationality (including and excluding UK nationals).

5. **UK Owners and Residents**:
   - `uk_owners_and_residents_ocuppation_viz`: Pie chart showing the top occupations for UK nationals residing in the UK.

**Charting Notes**:
- **Toggleable Charts**: Allow users to switch between multiple views, enhancing comparative analysis.
- **Custom Dimensions**: Dimensions (e.g., width and height) are adjusted for specific visualizations to improve readability.
- **Dynamic Data Input**: Processed data is fed directly into visualization functions, ensuring consistency with upstream transformations.

**Future Improvements**:
- Parameterize chart dimensions (e.g., width, height) to avoid hardcoding.
- Add interactivity to all visualizations, such as hover effects and drill-down capabilities.
- Automate labeling for toggleable charts based on input data attributes.
"""
# -- Companies data --
cities_viz = viz.create_toggleable_pie_charts([active_city, not_active_city],
                                              ['city', 'city'],
                                              ['size', 'size'],
                                              ['Active Companies by City', 'Not Active Companies by City'])

company_type_viz = viz.create_toggleable_bar_charts([active_company, not_active_company],
                                                    ['company_type',
                                                        'company_type'],
                                                    ['size', 'size'],
                                                    ['Active Companies by Type',
                                                        'Not Active Companies by Type'],
                                                    width=1000,
                                                    height=1200)
years_viz = viz.create_toggleable_pie_charts([active_years, not_active_years],
                                             ['Years_bracket', 'Years_bracket'],
                                             ['size', 'size'],
                                             ['Active Companies by Years Bracket', 'Not Active Companies by Years Bracket'])
# -- Officers data --
officer_roles_viz = viz.create_bar_chart(
    officer_roles_df, 'officer_role', 'size', 'Officer Roles Overview')
occupation_viz = viz.create_pie_chart(
    occupations_df, "occupation", "size", "Occupation Overview")
owners_viz = viz.create_toggleable_pie_charts([owners_df, owners_officer_roles],
                                              ['is_owner', 'officer_role'],
                                              ['size', 'size'],
                                              ['Is Owner', 'Owner Ofiicer Role'])
nationality_viz = viz.create_toggleable_bar_charts(
    [nationality_df, nationality_excl_uk],
    ['nationality', 'nationality'],
    ['size', 'size'],
    ['Nationality Overview', 'Nationality Overview (excl UK)']
)

residence_nationality_viz = viz.create_toggleable_bar_charts(
    [uk_residence_df, uk_nationality],
    ['nationality', 'country_of_residence'],
    ['size', 'size'],
    ['UK Residence (excl British)', 'UK Nationality (not UK resident)']
)

owners_and_non_owners_viz = viz.create_toggleable_pie_charts(
    [owners_nationality, owners_nationality_excl_uk,
        non_owners_nationality, non_owners_nationality_excl_uk],
    ['nationality', 'nationality', 'nationality', 'nationality'],
    ['size', 'size', 'size', 'size'],
    ['Owners Nationality',
        'Owners Nationality (Excl UK)', 'Non Owners Nationality', 'Non Owners Nationality (Excl UK)']
)
uk_owners_and_residents_ocuppation_viz = viz.create_pie_chart(
    uk_owners_and_residents_ocuppation, "occupation", "size", "Occupation for UK nationals who reside in the UK")
"""
Save Charts as HTML Components: Export visualizations for embedding.

This section saves each generated chart as an HTML component, allowing the charts to be embedded in web pages or combined into dashboards.

**Key Steps**:
1. **Export Individual Charts**:
   - Each chart is converted to an HTML string using the `to_html` method of the visualization object.
   - The `full_html=False` parameter ensures the output contains only the chart-specific HTML, without a full HTML document wrapper.
   - The `include_plotlyjs=False` parameter excludes Plotly.js scripts, which can be included separately to optimize loading when embedding multiple charts.

2. **Charts Exported**:
   - `cities_html`: Toggleable pie charts for active and not active companies by city.
   - `company_type_html`: Toggleable bar charts for active and not active companies by type.
   - `years_html`: Toggleable pie charts for active and not active companies by years bracket.
   - `officer_roles_html`: Bar chart summarizing officer roles.
   - `occupation_html`: Pie chart showing top occupations with aggregated "Other."
   - `owners_html`: Toggleable pie charts for ownership status and owner officer roles.
   - `nationality_html`: Toggleable bar charts for nationality overview (with and without UK nationals).
   - `residence_nationality_html`: Toggleable bar charts for:
       - Non-UK nationals residing in the UK.
       - UK nationals residing abroad.
   - `owners_and_non_owners_html`: Toggleable pie charts showing owners' and non-owners' nationality (with and without UK nationals).
   - `uk_owners_and_residents_ocuppation_html`: Pie chart for top occupations among UK nationals residing in the UK.

**Usage**:
- These HTML components can be embedded into static HTML files, dynamic dashboards, or web applications.

**Future Improvements**:
- Automate the naming of HTML variables based on the chart titles or types.
- Integrate a batch-saving mechanism to save all charts as separate HTML files in a specified directory.
- Explore additional export formats (e.g., PNG, PDF) for non-web-based use cases.
"""
# Save individual charts as HTML components
cities_html = cities_viz.to_html(full_html=False, include_plotlyjs=False)
company_type_html = company_type_viz.to_html(
    full_html=False, include_plotlyjs=False)
years_html = years_viz.to_html(full_html=False, include_plotlyjs=False)

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

"""
Generate and Save HTML Dashboard: Create an interactive web page with visualizations.

This section performs the following tasks:

1. **HTML Template Creation**:
   - Constructs an HTML template using Python string formatting.
   - Embeds interactive visualizations (previously generated as HTML components) into the template.
   - Utilizes Bootstrap for responsive styling and Plotly.js for rendering interactive charts.

2. **Template Features**:
   - **Header and Title**: Includes a title ("Company Analysis") and organizes charts into sections with headings.
   - **Styling**:
     - Includes Bootstrap CSS for consistent styling across devices.
     - Custom CSS is added for padding, margins, and layout optimization.
   - **Interactive Charts**:
     - Visualizations are embedded within `div` elements, categorized by topics such as:
       - Active Companies (Cities, Types, Years Bracket)
       - Officer Analysis (Roles, Occupation, Ownership)
       - Nationality and Residence Overview
     - HTML components for each chart (e.g., `{cities_html}`) are dynamically injected.

3. **HTML File Saving**:
   - The complete HTML content is saved to a file (`uk_exploration.html`) for distribution or direct use in a web browser.

4. **Console Output**:
   - Confirms the successful creation of the HTML file and outputs its name.

**Purpose**:
- Generate a standalone, shareable, interactive dashboard for exploring company and officer data.
- Enable easy access and visualization of key insights without requiring additional tools.

**Future Improvements**:
- Parameterize the file name (`html_file`) for flexibility.
- Modularize the template creation for easier maintenance (e.g., separate template from chart injection).
- Add metadata (e.g., descriptions, tooltips) for charts to enhance interpretability.
"""

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
            <div class="chart">{cities_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Company Types</h3>
            <div class="chart">{company_type_html}</div>
        </div>
        <div class="chart-container">
            <h3 class="text-center">Years Bracket</h3>
            <div class="chart">{years_html}</div>
        </div>
    </div>
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
html_file = "uk_exploration.html"
with open(html_file, "w") as file:
    file.write(html_template)
print(f"The updated HTML file with all charts has been saved as: {html_file}")
