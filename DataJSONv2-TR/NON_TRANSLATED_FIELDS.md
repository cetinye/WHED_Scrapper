# Non-Translated Fields

These values are intentionally preserved in their original form during the TR export.

## Applied Now

- `university_details.code`: Identifier code; machine-readable value.
- `data.general_information.university_name`: Official institution name; proper noun.
- `data.general_information.native_name`: Official native-language institution name; proper noun.
- `data.general_information.country`: Original location string from source data; kept as-is to avoid translating place names.
- `data.divisions.division_name`: Official division / faculty / school name; treated as an institutional proper name.
- `data.divisions.more_details`: Often contains campus names, person names, abbreviations, or location notes; kept as-is.
- `data.degree_fields.degree_field_title`: Formal degree title from source data; preserved to avoid mistranslating official award names.
- `data.location_information.street`: Postal address component; should stay in original form.
- `data.location_information.city`: City name; proper location name.
- `data.location_information.province`: State/province/region name; proper location name.
- `data.location_information.post_code`: Postal code; identifier value.
- `data.location_information.full_address`: Full postal address; should stay in original form.
- `data.contact_information.website`: URL; locator value.
- `data.contact_information.contact_page`: URL; locator value.
- `data.contact_information.email`: Email address; locator value.
- `data.contact_information.phone`: Phone number; locator value.
- `data.contact_information.phone_standardized`: Standardized phone number; locator value.
- `data.officers.name`: Person name; proper noun.
- `manifest[].state`: State label in generated manifest files; proper location name.

## Partial Translation Rules

- `data.contact_information.key_contacts`: Person names stay original; titles inside parentheses are translated.
- `data.officers.role`: Role labels are translated; person names are preserved.
- `data.officers.job_title`: Job titles are translated; person names are preserved separately.

## Pattern-Based Rules

- `URLs`: Any full URL value is preserved.
- `Emails`: Any full email value is preserved.
- `Codes`: IAU/WHED-style identifiers are preserved.
- `Numeric-only values`: Pure numeric / score / percentage-like strings are preserved.
- `File names and filesystem paths`: File names, manifest file references, and local paths are preserved.

## Review Candidates

- `data.general_information.history`: Usually safe to translate, but may contain former official institution names inside the sentence.
