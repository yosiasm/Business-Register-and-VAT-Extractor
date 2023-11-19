import pandas as pd
import re
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        # logging.FileHandler("debug.log"),
        logging.StreamHandler(sys.stdout)
    ],
)

# Company Sample
# find business registration and VAT number in dataset
logging.info(
    "[Company Sample]: finding business registration and VAT number in dataset"
)

### 1. Load Dataset
logging.info("1. Load Dataset")
# Load Company Sample
dataset_filename = "company_sample.csv"
company_sample = pd.read_csv(dataset_filename)
total_row = len(company_sample)

# remove empty snippet
company_sample.dropna(
    subset=[
        "gsearch_snippet1",
        "gsearch_snippet2",
        "gsearch_snippet3",
        "gsearch_snippet4",
        "gsearch_snippet5",
    ],
    how="all",
    inplace=True,
)
valid_row = len(company_sample)
print(dataset_filename)
print("total row:", total_row)
print("valid row:", valid_row)

### 2. Preprocess Dataset
logging.info("2. Preprocess Dataset")
# Merge Snippet
company_sample["FULL_SNIPPET"] = (
    company_sample["gsearch_snippet1"].astype(str)
    + " "
    + company_sample["gsearch_snippet2"].astype(str)
    + " "
    + company_sample["gsearch_snippet3"].astype(str)
    + " "
    + company_sample["gsearch_snippet4"].astype(str)
    + " "
    + company_sample["gsearch_snippet5"].astype(str)
)

# Make New Dataframe
comp_snippet = company_sample[["Company", "FULL_SNIPPET"]].copy()
comp_snippet.head(3)


# Remove Symbol in FULL SNIPPET
def preprocess(text: str):
    # join nummer
    text = text.replace("nummer.", "nummer ")
    text = text.replace("Nummer.", "Nummer ")
    text = text.replace(" nummer:", "-nummer:")
    text = text.replace(" Nummer:", "-Nummer:")
    # join nr
    text = (
        text.replace(" nr.:", "-nr:").replace(" nr. ", "-nr:").replace(" nr:", "-nr:")
    )
    # remove .
    # text = text.replace('.', '')
    text = re.sub("(\w)\.(\w)", "", text)

    # remove multiple space
    text = re.sub(" +", " ", text)
    return text


comp_snippet["CLEAN_SNIPPET"] = comp_snippet["FULL_SNIPPET"].apply(preprocess)


# from comp_snippet we will create two branch, comp_snippet_br for
# business register and comp_snippet_vat for vat.
# They will be merged into a single dataframe in the end

# Business Register
logging.info("[Business Register]")
### 1. Load Base Knowledge
logging.info("1. Load Base Knowledge")
knowledge = pd.read_csv("KNOWLEDGE.csv")

### 2. Find Business Register
logging.info("2. Find Business Register")


def find_business_register(text):
    registers = []
    for key, row in knowledge.iterrows():
        if (
            row["business_register_abb"].lower() in text.lower()
            or row["business_register"].lower() in text.lower()
        ):
            registers.append(row["business_register_abb"])
    return registers if registers else None


comp_snippet["BUSINESS_REGISTER"] = comp_snippet["CLEAN_SNIPPET"].apply(
    find_business_register
)
comp_snippet_br = comp_snippet.explode(["BUSINESS_REGISTER"])

### 3. Find Business Register Country
logging.info("3. Find Business Register Country")
comp_snippet_br = comp_snippet_br.merge(
    knowledge[["business_register_abb", "business_register", "country"]],
    left_on="BUSINESS_REGISTER",
    right_on="business_register_abb",
    how="left",
)

### 4. Find Business Register Number
logging.info("4. Find Business Register Number")


def find_br_number(row):
    results = []
    breg_abb = row["BUSINESS_REGISTER"]
    breg = row["business_register"]

    # loop trough business register abbrivation and long version
    bregs = [
        breg_abb,
        f"{breg_abb}-nummer",
        f"{breg_abb}-nr",
        breg,
        f"{breg}-nummer",
        f"{breg}-nr",
    ]

    for br in bregs:
        pattern = rf"(?i){br}\s*:?\s*([\w\s]+)"
        results.extend(re.findall(pattern, row["CLEAN_SNIPPET"]))

    clean_results = []
    for res in results:
        # remove word
        pattern = r"\b(?:[a-zA-Z]{4,})\b"
        res = re.sub(pattern, "", res)

        # remove lowercase word
        pattern = r"\b(?:[a-z]+)\b"
        res = re.sub(pattern, "", res)

        # remove nan
        res = res.replace("nan", "").strip()

        if len(res) > 2:
            clean_results.append(res)
    return clean_results


comp_snippet_br["BUSINESS_REGISTER_NUM"] = comp_snippet_br.apply(find_br_number, axis=1)

comp_snippet_br_expl = comp_snippet_br.explode(["BUSINESS_REGISTER_NUM"])
print("total row with business_register:", len(comp_snippet_br_expl))


# VAT Number
logging.info("[VAT Number]")
### 1. Load Knowledge Base
logging.info("1. Load Knowledge Base")
# Load VAT Rules
vat_rules = pd.read_csv(
    "VAT_RULES.csv", delimiter=";", encoding="UTF-8-SIG", dtype={"char": int}
)

### 2. Preprocess Knowledge Base
logging.info("2. Preprocess Knowledge Base")


# Regex to filter VAT
def create_regex(row):
    code_format = row["code_format"]
    char_digit = row["char"]
    # regex = rf'\b{code_format}\s?[A-Z0-9]{{{char_digit}}}\b'
    regex = rf"\b{code_format}(?:\s?[A-Z\d]{{1,{char_digit}}})+\b"

    return re.compile(regex)


vat_rules["VAT_REGEX"] = vat_rules.apply(create_regex, axis=1)

### 3. Find VAT
logging.info("3. Find VAT")
# Create Function to filter VAT
patterns = vat_rules["VAT_REGEX"].to_list()


def find_vat_number(text):
    results = []
    if isinstance(text, str):
        text = text.replace(". ", " . ")

        for pattern in patterns:
            matches = pattern.findall(text)
            results.extend(matches)

    # remove duplicate
    clean_results = [res.strip() for res in results]
    clean_results = list(set(clean_results))

    # remove word
    pattern = r"\b(?:[A-Z]{4,})\b"
    clean_results = [re.sub(pattern, "", res).strip() for res in clean_results]

    # remove below 3 digits
    clean_results = [res for res in clean_results if len(res) > 3]

    return clean_results


comp_snippet_va = comp_snippet.copy()
comp_snippet_va["VAT_NUMBER"] = comp_snippet_va["CLEAN_SNIPPET"].apply(find_vat_number)
comp_snippet_va_expl = comp_snippet_va.explode("VAT_NUMBER")

### 4. Find VAT Country
logging.info("4. Find VAT Country")


def find_vat_country(text):
    if isinstance(text, str):
        pattern = r"^[^\s^\d]+"

        match = re.search(pattern, text)
        if match:
            result = match.group(0)
            return result


comp_snippet_va_expl["code_format"] = comp_snippet_va_expl["VAT_NUMBER"].apply(
    find_vat_country
)
comp_snippet_va_expl = comp_snippet_va_expl.merge(
    vat_rules[["country", "code_format"]],
    left_on="code_format",
    right_on="code_format",
    how="left",
)

# Merge Business Register and VAT dataset
logging.info("[Merge Business Register and VAT dataset]")
### 1. Merging Dataset
logging.info("1. Merging Dataset")
br_df = comp_snippet_br_expl[
    ["country", "Company", "BUSINESS_REGISTER", "BUSINESS_REGISTER_NUM"]
]
va_df = comp_snippet_va_expl[["country", "Company", "code_format", "VAT_NUMBER"]]
company_detail = pd.merge(br_df, va_df, how="outer", on=["country", "Company"])
company_detail.drop_duplicates(inplace=True)

### 2. Find VAT Number Name
logging.info("2. Find VAT Number Name")
company_detail = company_detail.merge(
    knowledge[["country", "vat_abb"]], left_on="country", right_on="country"
)
company_detail = company_detail[
    [
        "country",
        "Company",
        "BUSINESS_REGISTER",
        "BUSINESS_REGISTER_NUM",
        "vat_abb",
        "VAT_NUMBER",
    ]
]

company_detail.drop_duplicates(inplace=True)

### 3. Change Column Name
logging.info("3. Change Column Name")
company_detail.rename(
    columns={
        "country": "Country",
        "BUSINESS_REGISTER": "Business register name",
        "BUSINESS_REGISTER_NUM": "Business number",
        "vat_abb": "VAT number name",
        "VAT_NUMBER": "VAT number",
    },
    inplace=True,
)

# Save to CSV
logging.info("[Save to CSV]")
company_detail.to_csv("company_detail.csv", index=False)

logging.info("Done!")
