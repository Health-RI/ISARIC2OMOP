import pandas as pd
from utils import increment_last_id
from typing import Dict

from core.db_connector import PostgresController

ISARIC_COUNTRY_CODES = {"1": "Afghanistan", "2": "Albania", "3": "Algeria", "4": "Andorra", "5": "Angola",
                        "6": "Antigua & Deps", "7": "Argentina", "8": "Armenia", "9": "Australia", "10": "Austria",
                        "11": "Azerbaijan", "12": "Bahamas", "13": "Bahrain", "14": "Bangladesh", "15": "Barbados",
                        "16": "Belarus", "17": "Belgium", "18": "Belize", "19": "Benin", "20": "Bhutan",
                        "21": "Bolivia", "22": "Bosnia Herzegovina", "23": "Botswana", "24": "Brazil", "25": "Brunei",
                        "26": "Bulgaria", "27": "Burkina", "28": "Burundi", "29": "Cambodia", "30": "Cameroon",
                        "31": "Canada", "32": "Cape Verde", "33": "Central African Rep", "34": "Chad", "35": "Chile",
                        "36": "China", "37": "Colombia", "38": "Comoros", "39": "Congo", "40": "Congo {Democratic Rep}",
                        "41": "Costa Rica", "42": "Croatia", "43": "Cuba", "44": "Cyprus", "45": "Czech Republic",
                        "46": "Denmark", "47": "Djibouti", "48": "Dominica", "49": "Dominican Republic",
                        "50": "East Timor", "51": "Ecuador", "52": "Egypt", "53": "El Salvador",
                        "54": "Equatorial Guinea", "55": "Eritrea", "56": "Estonia", "57": "Ethiopia", "58": "Fiji",
                        "59": "Finland", "60": "France", "61": "Gabon", "62": "Gambia", "63": "Georgia",
                        "64": "Germany", "65": "Ghana", "66": "Greece", "67": "Grenada", "68": "Guatemala",
                        "69": "Guinea", "70": "Guinea-Bissau", "71": "Guyana", "72": "Haiti", "73": "Honduras",
                        "74": "Hungary", "75": "Iceland", "76": "India", "77": "Indonesia", "78": "Iran", "79": "Iraq",
                        "80": "Ireland {Republic}", "81": "Israel", "82": "Italy", "83": "Ivory Coast", "84": "Jamaica",
                        "85": "Japan", "86": "Jordan", "87": "Kazakhstan", "88": "Kenya", "89": "Kiribati",
                        "90": "Korea North", "91": "Korea South", "92": "Kosovo", "93": "Kuwait", "94": "Kyrgyzstan",
                        "95": "Laos", "96": "Latvia", "97": "Lebanon", "98": "Lesotho", "99": "Liberia", "100": "Libya",
                        "101": "Liechtenstein", "102": "Lithuania", "103": "Luxembourg", "104": "Macedonia",
                        "105": "Madagascar", "106": "Malawi", "107": "Malaysia", "108": "Maldives", "109": "Mali",
                        "110": "Malta", "111": "Marshall Islands", "112": "Mauritania",
                        "113": "Mauritius",
                        "114": "Mexico", "115": "Micronesia", "116": "Moldova", "117": "Monaco", "118": "Mongolia",
                        "119": "Montenegro", "120": "Morocco", "121": "Mozambique", "122": "Myanmar", "123": "Namibia",
                        "124": "Nauru", "125": "Nepal", "126": "Netherlands", "127": "New Zealand", "128": "Nicaragua",
                        "129": "Niger", "130": "Nigeria", "131": "Norway", "132": "Oman", "133": "Pakistan",
                        "134": "Palau",
                        "135": "Panama", "136": "Papua New Guinea", "137": "Paraguay", "138": "Peru",
                        "139": "Philippines",
                        "140": "Poland", "141": "Portugal", "142": "Qatar", "143": "Romania",
                        "144": "Russian Federation",
                        "145": "Rwanda", "146": "St Kitts & Nevis", "147": "St Lucia",
                        "148": "Saint Vincent & the Grenadines", "149": "Samoa", "150": "San Marino",
                        "151": "Sao Tome & Principe", "152": "Saudi Arabia", "153": "Senegal", "154": "Serbia",
                        "155": "Seychelles", "156": "Sierra Leone", "157": "Singapore", "158": "Slovakia",
                        "159": "Slovenia",
                        "160": "Solomon Islands", "161": "Somalia", "162": "South Africa", "163": "South Sudan",
                        "164": "Spain", "165": "Sri Lanka", "166": "Sudan", "167": "Suriname", "168": "Swaziland",
                        "169": "Sweden", "170": "Switzerland", "171": "Syria", "172": "Taiwan", "173": "Tajikistan",
                        "174": "Tanzania", "175": "Thailand", "176": "Togo", "177": "Tonga", "178": "Trinidad & Tobago",
                        "179": "Tunisia", "180": "Turkey", "181": "Turkmenistan", "182": "Tuvalu", "183": "Uganda",
                        "184": "Ukraine", "185": "United Arab Emirates", "186": "United Kingdom",
                        "187": "United States",
                        "188": "Uruguay", "189": "Uzbekistan", "190": "Vanuatu", "191": "Vatican City",
                        "192": "Venezuela",
                        "193": "Vietnam", "194": "Yemen", "195": "Zambia", "196": "Zimbabwe", "197": "Other"}


def get_location_data(postgres: PostgresController) -> Dict[str, int]:
    """
    Fetches location data from concept. NB! valid with Athena vocabularies CDM V5 as of V5.0 29-FEB-24
    Location.country_concept_id
    User Guidance: The Concept Id representing the country. Values should conform to the Geography domain.
    :param postgres: DB client
    :return: dictionary of concept_name (country) as a key and concept_id as value
    """
    query = (f"SELECT * FROM {postgres.schema}.concept "
             f"WHERE domain_id = 'Geography' and standard_concept='S' and concept_class_id = 'Location';")
    result = postgres.postgres_fetch(query)
    location_dict = pd.Series(result["concept_id"].values, index=result["concept_name"]).to_dict()
    return location_dict


def populate_location(location_df: pd.DataFrame, postgres: PostgresController) -> Dict[str, int]:
    """
    Transformation rules are set as per https://omop-erd.surge.sh/omop_cdm/tables/LOCATION.html
    """
    # Select location concept
    dict_location = get_location_data(postgres)

    location_header = ["location_id", "address_1", "address_2", "city", "state", "zip", "county",
                       "location_source_value", "country_concept_id", "country_source_value", "latitude",
                       "longitude"]
    # Prepare location data
    location_df.rename(columns={"country": "location_source_value"}, inplace=True)
    # Location.location_source_value
    # ETL Conventions: Put the verbatim value for the location here, as it shows up in the source.
    location_df["location_source_value"] = location_df["location_source_value"].apply(lambda x: str(int(x)))
    # Location.country_source_value User Guidance: The name of the country.
    location_df["country_source_value"] = location_df["location_source_value"].apply(
        lambda x: ISARIC_COUNTRY_CODES[x] if pd.notnull(x) else x)
    location_df["country_concept_id"] = location_df["country_source_value"].apply(
        lambda x: dict_location.get(x) if pd.notnull(x) else x)
    location_df = location_df.reindex(columns=location_header)
    # Select rows to insert if they are exact duplicates (excluding location_id)
    existing_locations = (postgres.postgres_fetch(query=f"SELECT * from {postgres.schema}.location",
                                                  column_names=location_header))
    if not existing_locations.empty:
        location_ids_dict = pd.Series(existing_locations["location_id"].values,
                                      index=existing_locations["location_source_value"].astype(str)).to_dict()
        existing_locations = existing_locations.drop(columns=["location_id"])
        insert_locations = pd.merge(left=location_df.drop(columns=["location_id"]).astype(existing_locations.dtypes),
                                    right=existing_locations,
                                    indicator=True,
                                    how='outer').query('_merge=="left_only"').drop('_merge', axis=1)
    else:
        insert_locations = location_df.drop(columns=["location_id"])
        location_ids_dict = {}

    if not insert_locations.empty:
        increment_by_index = increment_last_id("location", "location_id", postgres)
        insert_locations.index += increment_by_index
        insert_locations.index.name = "location_id"
        insert_locations.reset_index(drop=False, inplace=True)
        postgres.df_to_postgres(table="location", df=insert_locations)
        location_ids_dict.update(pd.Series(insert_locations["location_id"].values,
                                           index=insert_locations["location_source_value"].astype(str)).to_dict())

    return location_ids_dict


def get_locations(postgres: PostgresController) -> Dict[str, int]:
    """
    :param postgres: db client
    :return: a dictionary with source (e.g. ISARIC) location value (as a string) as key and location id as a value
    """
    existing_locations = (
        postgres.postgres_fetch(query=f"SELECT location_id, location_source_value from {postgres.schema}.location",
                                column_names=["location_id", "location_source_value"]))
    location_ids_dict = pd.Series(existing_locations["location_id"].values,
                                  index=existing_locations["location_source_value"].astype(str)).to_dict()
    return location_ids_dict
