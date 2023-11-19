# Company Sample
find business registration and VAT number in dataset
Using pandas and regex to process data

python version
dataset: `company_sample.csv`
requirements: `pip install -r requirements.txt`
python script: `business_register_and_vat.py`
output: `company_detail.csv`

### 1. Load Dataset
- Load Company Sample
- remove empty snippet

### 2. Preprocess Dataset
- Merge Snippet
- Make New Dataframe
- Remove Symbol in FULL SNIPPET

from comp_snippet we will create two branch, comp_snippet_br for business register and comp_snippet_vat for vat. They will be merged into a single dataframe in the end

# Business Register
### 1. Load Base Knowledge
### 2. Find Business Register
### 3. Find Business Register Country
### 4. Find Business Register Number

# VAT Number
### 1. Load Knowledge Base
- Load VAT Rules
### 2. Preprocess Knowledge Base
- Regex to filter VAT
### 3. Find VAT
- Create Function to filter VAT
### 4. Find VAT Country

# Merge Business Register and VAT dataset
### 1. Merging Dataset
### 2. Find VAT Number Name
### 3. Change Column Name

# Save to CSV
