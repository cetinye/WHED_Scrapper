from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple


ROOT_DIR = Path(__file__).resolve().parent
ISCED_SUBJECT_INDEX_PATH = ROOT_DIR / "References" / "Codes" / "isced_f_subject_index.json"
BACHELOR_PROGRAM_MAP_PATH = ROOT_DIR / "References" / "Codes" / "bachelor_program_iscedf_map.json"

MULTISPACE_RE = re.compile(r"\s+")

FIELD_NAMES = {
    "0031": "Personal skills and development",
    "0110": "Education not further defined",
    "0111": "Education science",
    "0112": "Training for pre-school teachers",
    "0113": "Teacher training without subject specialisation",
    "0114": "Teacher training with subject specialisation",
    "0188": "Inter-disciplinary programmes and qualifications involving education",
    "0200": "Arts and humanities not further defined",
    "0210": "Arts not further defined",
    "0211": "Audio-visual techniques and media production",
    "0212": "Fashion, interior and industrial design",
    "0213": "Fine arts",
    "0214": "Handicrafts",
    "0215": "Music and performing arts",
    "0220": "Humanities not further defined",
    "0221": "Religion and theology",
    "0222": "History and archaeology",
    "0223": "Philosophy and ethics",
    "0230": "Languages not further defined",
    "0231": "Language acquisition",
    "0232": "Literature and linguistics",
    "0288": "Inter-disciplinary programmes and qualifications involving arts and humanities",
    "0300": "Social sciences, journalism and information not further defined",
    "0310": "Social and behavioural sciences not further defined",
    "0311": "Economics",
    "0312": "Political sciences and civics",
    "0313": "Psychology",
    "0314": "Sociology and cultural studies",
    "0320": "Journalism and information not further defined",
    "0321": "Journalism and reporting",
    "0322": "Library, information and archival studies",
    "0388": "Inter-disciplinary programmes and qualifications involving social sciences, journalism and information",
    "0400": "Business, administration and law not further defined",
    "0410": "Business and administration not further defined",
    "0411": "Accounting and taxation",
    "0412": "Finance, banking and insurance",
    "0413": "Management and administration",
    "0414": "Marketing and advertising",
    "0415": "Secretarial and office work",
    "0416": "Wholesale and retail sales",
    "0417": "Work skills",
    "0421": "Law",
    "0488": "Inter-disciplinary programmes and qualifications involving business, administration and law",
    "0500": "Natural sciences, mathematics and statistics not further defined",
    "0510": "Biological and related sciences not further defined",
    "0511": "Biology",
    "0512": "Biochemistry",
    "0519": "Biological and related sciences not elsewhere classified",
    "0520": "Environment not further defined",
    "0521": "Environmental sciences",
    "0522": "Natural environments and wildlife",
    "0530": "Physical sciences not further defined",
    "0531": "Chemistry",
    "0532": "Earth sciences",
    "0533": "Physics",
    "0540": "Mathematics and statistics not further defined",
    "0541": "Mathematics",
    "0542": "Statistics",
    "0588": "Inter-disciplinary programmes and qualifications involving natural sciences, mathematics and statistics",
    "0610": "Information and Communication Technologies (ICTs) not further defined",
    "0611": "Computer use",
    "0612": "Database and network design and administration",
    "0613": "Software and applications development and analysis",
    "0688": "Inter-disciplinary programmes and qualifications involving Information and Communication Technologies (ICTs)",
    "0700": "Engineering, manufacturing and construction not further defined",
    "0710": "Engineering and engineering trades not further defined",
    "0711": "Chemical engineering and processes",
    "0712": "Environmental protection technology",
    "0713": "Electricity and energy",
    "0714": "Electronics and automation",
    "0715": "Mechanics and metal trades",
    "0716": "Motor vehicles, ships and aircraft",
    "0719": "Engineering and engineering trades not elsewhere classified",
    "0720": "Manufacturing and processing not further defined",
    "0721": "Food processing",
    "0722": "Materials (glass, paper, plastic and wood)",
    "0723": "Textiles (clothes, footwear and leather)",
    "0724": "Mining and extraction",
    "0730": "Architecture and construction not further defined",
    "0731": "Architecture and town planning",
    "0732": "Building and civil engineering",
    "0788": "Inter-disciplinary programmes and qualifications involving engineering, manufacturing and construction",
    "0800": "Agriculture, forestry, fisheries and veterinary not further defined",
    "0810": "Agriculture not further defined",
    "0811": "Crop and livestock production",
    "0812": "Horticulture",
    "0821": "Forestry",
    "0831": "Fisheries",
    "0841": "Veterinary",
    "0888": "Inter-disciplinary programmes and qualifications involving agriculture, forestry, fisheries and veterinary",
    "0900": "Health and welfare not further defined",
    "0910": "Health not further defined",
    "0911": "Dental studies",
    "0912": "Medicine",
    "0913": "Nursing and midwifery",
    "0914": "Medical diagnostic and treatment technology",
    "0915": "Therapy and rehabilitation",
    "0916": "Pharmacy",
    "0917": "Traditional and complementary medicine and therapy",
    "0919": "Health not elsewhere classified",
    "0920": "Welfare not further defined",
    "0921": "Care of the elderly and of disabled adults",
    "0922": "Child care and youth services",
    "0923": "Social work and counselling",
    "0988": "Inter-disciplinary programmes and qualifications involving health and welfare",
    "1000": "Services not further defined",
    "1011": "Domestic services",
    "1012": "Hair and beauty services",
    "1013": "Hotel, restaurants and catering",
    "1014": "Sports",
    "1015": "Travel, tourism and leisure",
    "1021": "Community sanitation",
    "1022": "Occupational health and safety",
    "1031": "Military and defence",
    "1032": "Protection of persons and property",
    "1041": "Transport services",
    "1088": "Inter-disciplinary programmes and qualifications involving services",
}

STOP_WORDS = {
    "and",
    "the",
    "of",
    "in",
    "for",
    "with",
    "study",
    "studies",
    "science",
    "sciences",
    "programme",
    "programmes",
    "program",
    "programs",
    "qualification",
    "qualifications",
    "field",
    "fields",
    "specialisation",
    "specialization",
    "specialised",
    "specialized",
    "speciality",
    "specialty",
    "honours",
    "honors",
    "degree",
    "degrees",
    "bachelor",
    "bachelors",
    "undergraduate",
    "graduate",
    "postgraduate",
    "post",
    "diploma",
    "certificate",
    "certificates",
    "associate",
    "master",
    "masters",
    "doctor",
    "doctoral",
    "doctorate",
    "licence",
    "licenciat",
    "licenc",
    "maestrado",
    "mestrado",
    "magistr",
    "kandidat",
    "specialista",
    "specialist",
    "doktor",
    "phd",
    "ph",
    "d",
    "ba",
    "bsc",
    "bs",
    "msc",
    "ma",
    "llb",
    "llm",
    "md",
    "mba",
    "bba",
    "offered",
    "by",
    "also",
    "year",
    "years",
    "yr",
    "yrs",
    "after",
    "per",
    "subject",
}

KNOWN_LANGUAGES = {
    "albanian",
    "arabic",
    "armenian",
    "afrikaans",
    "bulgarian",
    "catalan",
    "chinese",
    "czech",
    "danish",
    "dutch",
    "english",
    "estonian",
    "filipino",
    "finnish",
    "french",
    "german",
    "greek",
    "hausa",
    "hebrew",
    "hindi",
    "hungarian",
    "igbo",
    "indonesian",
    "irish",
    "italian",
    "japanese",
    "korean",
    "kurdish",
    "latin",
    "lithuanian",
    "malay",
    "maltese",
    "mongolian",
    "norwegian",
    "persian",
    "polish",
    "portuguese",
    "punjabi",
    "romanian",
    "russian",
    "sanskrit",
    "serbocroatian",
    "slovenian",
    "spanish",
    "swahili",
    "swedish",
    "tamil",
    "tibetan",
    "turkish",
    "urdu",
    "uzbek",
    "vietnamese",
    "yoruba",
}

LANGUAGE_GROUPS = {
    "african languages",
    "ancient languages",
    "asian languages",
    "austronesian and oceanic languages",
    "amerindian languages",
    "baltic languages",
    "eurasian and north asian languages",
    "european languages",
    "foreign languages",
    "germanic languages",
    "indic languages",
    "modern languages",
    "oriental languages",
    "romance languages",
    "scandinavian languages",
    "slavic languages",
    "south and southeast asian languages",
    "thai languages",
}

REGIONAL_STUDIES = {
    "african american studies",
    "african studies",
    "american studies",
    "asian studies",
    "east asian studies",
    "european studies",
    "latin american studies",
    "middle eastern studies",
    "regional studies",
}

RAW_TITLE_EXACT_OVERRIDES = {
    "A Master’s degree ¨Programme in Exercise Science is to be launched in the summer of 2024": "1014",
    "child health": "0919",
    "Conservatory Programmes": "0215",
    "Dual Bachelor's degree in Engineering offered with Washington University": "0710",
    "Educator Preparation Institute": "0111",
    "English Training Supplement": "0231",
    "European Master in System Dynamics": "0541",
    "Language Programmes": "0231",
    "Pre-Med": "0912",
    "Safety Engineering Magistr": "1022",
    "Some Programmes are taught in English": "0231",
    "some Programmes offered in English Language": "0231",
    "Teaching Licensure": "0111",
    "Top-up programme for Indian and Pakistan Nurses": "0913",
    "Viticulture Mestre": "0812",
    "with with the McKelvey School of Engineering at Washington University": "0710",
}

RAW_EXACT_OVERRIDES = {
    "Accountancy": "0411",
    "Acting": "0215",
    "Advertising and Publicity": "0414",
    "Aeronautical and Aerospace Engineering": "0716",
    "Agrobiology": "0811",
    "Agricultural Equipment": "0716",
    "Air Transport": "1041",
    "Ancient Books": "0232",
    "Ancient Civilizations": "0222",
    "Anaesthesiology": "0912",
    "Applied Chemistry": "0531",
    "Applied Linguistics": "0232",
    "Applied Physics": "0533",
    "Architecture and Planning": "0731",
    "Architectural and Environmental Design": "0731",
    "Archiving": "0322",
    "Arts and Humanities": "0200",
    "Astronomy and Space Science": "0533",
    "Banking": "0412",
    "Behavioural Sciences": "0310",
    "Biological and Life Sciences": "0510",
    "Biomedicine": "0912",
    "Bioengineering": "0719",
    "Bible": "0221",
    "Broadcasting": "0321",
    "Business and Commerce": "0410",
    "Business Computing": "0688",
    "Ceramic Art": "0214",
    "Child Care and Development": "0922",
    "Chiropractic": "0917",
    "Cinema and Television": "0211",
    "Clinical Psychology": "0313",
    "Clothing and Sewing": "0723",
    "Communication Arts": "0321",
    "Communication Disorders": "0915",
    "Communication Studies": "0321",
    "Community Health": "0919",
    "Comparative Politics": "0312",
    "Construction Engineering": "0732",
    "Cooking and Catering": "1013",
    "Crafts and Trades": "0214",
    "Crop Production": "0811",
    "Cybersecurity": "0612",
    "Data Processing": "0610",
    "Dietetics": "0915",
    "Display and Stage Design": "0212",
    "Distance Education": "0111",
    "Documentation Techniques": "0322",
    "E-Business Commerce": "0410",
    "Educational Psychology": "0313",
    "Engineering": "0710",
    "Engineering Drawing and Design": "0732",
    "Engineering Management": "0413",
    "Engraving": "0213",
    "Ergotherapy": "0915",
    "Esoteric Practices": "0221",
    "Family Studies": "0923",
    "Film": "0211",
    "Fishery": "0831",
    "Food Science": "0721",
    "Food Technology": "0721",
    "Forest Products": "0821",
    "Furniture Design": "0212",
    "Geochemistry": "0532",
    "Government": "0312",
    "Grammar": "0232",
    "Graphic Arts": "0211",
    "Harvest Technology": "0811",
    "Heritage Preservation": "0222",
    "Higher Education": "0111",
    "Higher Education Teacher Training": "0114",
    "Holy Writings": "0221",
    "Hygiene": "1021",
    "Industrial Chemistry": "0531",
    "Industrial Engineering": "0719",
    "Industrial Maintenance": "0715",
    "Industrial Management": "0413",
    "Information Technology": "0610",
    "International Business": "0410",
    "International Studies": "0312",
    "Irrigation": "0811",
    "Jazz and Popular Music": "0215",
    "Jewellery Art": "0214",
    "Justice Administration": "1032",
    "Juilliard": "0215",
    "Kinesiology": "1014",
    "KU Leuven": "0000",
    "Koran": "0221",
    "Labour and Industrial Relations": "0417",
    "Laboratory Techniques": "0711",
    "Leadership": "0031",
    "Leisure Studies": "1015",
    "Linguistics": "0232",
    "Logistics Management": "0413",
    "Maintenance Technology": "0719",
    "Mesterfokozat": "0000",
    "Materials Engineering": "0722",
    "Measurement and Precision Engineering": "0715",
    "Meat and Poultry": "0721",
    "Media Studies": "0321",
    "Medical Parasitology": "0912",
    "Microelectronics": "0714",
    "Music Theory and Composition": "0215",
    "Musical Instruments": "0214",
    "Multimedia": "0211",
    "Natural Resources": "0522",
    "Natural Sciences": "0500",
    "Native Language": "0232",
    "Neurosciences": "0519",
    "New Testament": "0221",
    "Occupational Health": "1022",
    "Oenology": "0721",
    "Opera": "0215",
    "Operations Research": "0541",
    "Orthopaedics": "0912",
    "Paleontology": "0532",
    "Painting and Drawing": "0213",
    "Parks and Recreation": "1015",
    "Pedagogy": "0111",
    "Periodontics": "0911",
    "Petroleum and Gas Engineering": "0724",
    "Philosophical Schools": "0223",
    "Physical Therapy": "0915",
    "Plant Pathology": "0811",
    "Political Sciences": "0312",
    "Podiatry": "0912",
    "Portuguese Language and Culture Studies for foreigners": "0231",
    "Prehistory": "0222",
    "Preschool": "0112",
    "Preschool Education": "0112",
    "Primary Education": "0113",
    "Printing and Printmaking": "0211",
    "Private Law": "0421",
    "Production Engineering": "0715",
    "Protective Services": "1032",
    "Prosthetics and Orthotics": "0914",
    "Psycholinguistics": "0232",
    "Psychometrics": "0313",
    "Public Health": "0919",
    "Public Law": "0421",
    "Primitive Religions": "0221",
    "Radio and Television Broadcasting": "0211",
    "Radiophysics": "0533",
    "Real Estate": "0416",
    "Rehabilitation and Therapy": "0915",
    "Restoration of Works of Art": "0222",
    "Retailing and Wholesaling": "0416",
    "Rubber Technology": "0722",
    "Rural Planning": "0731",
    "Sales Techniques": "0416",
    "Secretarial Studies": "0415",
    "Secondary Education": "0114",
    "Service Trades": "1000",
    "Singing": "0215",
    "Small Business": "0410",
    "Social and Community Services": "0923",
    "Social Sciences": "0300",
    "Social Problems": "0314",
    "Social Welfare": "0923",
    "Software Engineering": "0613",
    "Special Education": "0113",
    "Speech Studies": "0232",
    "Speech Therapy and Audiology": "0915",
    "Staff Development": "0417",
    "STEM": "0588",
    "Sustainable Development": "0521",
    "Systems Analysis": "0613",
    "Taxation": "0411",
    "Telecommunications Engineering": "0714",
    "Town Planning": "0731",
    "Translation and Interpretation": "0231",
    "Transport Engineering": "0716",
    "Treatment Techniques": "0914",
    "Tropical Agriculture": "0811",
    "Urology": "0912",
    "Vegetable Production": "0811",
    "Cattle Breeding": "0811",
    "Conducting": "0215",
    "Fruit Production": "0811",
    "Greek Classical": "0231",
    "Health Sciences": "0910",
    "Metaphysics": "0223",
    "Microwaves": "0714",
    "Technology": "0710",
    "Video": "0211",
    "Visual Arts": "0213",
    "Vocational Education": "0114",
    "Water Management": "0521",
    "Water Science": "0521",
    "Weaving": "0214",
    "Welfare and Protective Services": "0988",
    "Writing": "0232",
    "Yoga": "0917",
}

PATTERN_RULES: Sequence[Tuple[re.Pattern[str], str]] = (
    (re.compile(r"\b(accountancy|accounting|taxation|tax|fiscal)\b"), "0411"),
    (re.compile(r"\b(finance|banking|insurance|investment|stock|actuarial)\b"), "0412"),
    (re.compile(r"\b(marketing|advertising|publicity|public relations|merchandising)\b"), "0414"),
    (re.compile(r"\b(secretar|office|receptionist|stenography|shorthand|keyboard|switchboard)\b"), "0415"),
    (re.compile(r"\b(retail|sales|procurement|purchasing|real estate|property sales|wholesale|commerce)\b"), "0416"),
    (re.compile(r"\b(logistic|human resources|personnel|employment|recruitment|administration|management|entrepreneur|business)\b"), "0413"),
    (re.compile(r"\b(law|legal|juris|constitution|sharia)\b"), "0421"),
    (re.compile(r"\b(economics?|econometrics?)\b"), "0311"),
    (re.compile(r"\b(international relations?|diplomacy|politic|government|public policy|peace)\b"), "0312"),
    (re.compile(r"\b(psychology|psychotherapy|psychoanalysis|counselling|counseling)\b"), "0313"),
    (re.compile(r"\b(anthropology|sociology|cultural studies|development studies|regional studies|women s studies|gender studies|american studies|african studies|asian studies|european studies|middle eastern studies|east asian studies)\b"), "0314"),
    (re.compile(r"\b(journalism|reporting|mass communication|communication|media|broadcast|publishing)\b"), "0321"),
    (re.compile(r"\b(librar|information science|information studies|museum|museology|archiv)\b"), "0322"),
    (re.compile(r"\b(graphic|multimedia|film|video|television|radio|printing|photography)\b"), "0211"),
    (re.compile(r"\b(design|interior|fashion|display|stage design)\b"), "0212"),
    (re.compile(r"\b(fine arts|visual arts|painting|drawing|sculpture|printmaking|engraving|art criticism)\b"), "0213"),
    (re.compile(r"\b(crafts?|weaving|ceramic|jewellery|jewelry|instrument making)\b"), "0214"),
    (re.compile(r"\b(music|singing|opera|theatre|theater|drama|acting|dance|performing arts|jazz)\b"), "0215"),
    (re.compile(r"\b(religion|theology|religious|bible|pastoral|islamic studies|jewish studies|christian)\b"), "0221"),
    (re.compile(r"\b(history|archaeology|heritage|civilizations?|folklore)\b"), "0222"),
    (re.compile(r"\b(philosophy|ethics|logic|aesthetics)\b"), "0223"),
    (re.compile(r"\b(translation|interpretation|foreign language|modern languages|language acquisition)\b"), "0231"),
    (re.compile(r"\b(linguistics|literature|creative writing|writing|speech studies|philology|grammar)\b"), "0232"),
    (re.compile(r"\b(biology|botany|zoology|genetics|microbiology|marine biology|life sciences?)\b"), "0511"),
    (re.compile(r"\b(biochemistry|bioinformatics|molecular biology|biotechnology)\b"), "0512"),
    (re.compile(r"\b(environment|ecology|sustainable development)\b"), "0521"),
    (re.compile(r"\b(wildlife|conservation|natural resources|national parks)\b"), "0522"),
    (re.compile(r"\b(chemistry|chemical science)\b"), "0531"),
    (re.compile(r"\b(geology|geography|geophysics|geodesy|geomatics|hydrology|oceanography|marine science|earth science|water)\b"), "0532"),
    (re.compile(r"\b(physics|astronomy|space science|astrophysics|acoustics|radiophysics)\b"), "0533"),
    (re.compile(r"\b(mathematics?|algebra|geometry|numerical analysis|applied mathematics|operations research)\b"), "0541"),
    (re.compile(r"\b(statistics?|data science|actuarial)\b"), "0542"),
    (re.compile(r"\b(information technology|data processing)\b"), "0610"),
    (re.compile(r"\b(network|database|cybersecurity|it administration|computer networks?)\b"), "0612"),
    (re.compile(r"\b(software|informatics|computer science|programming|systems analysis|artificial intelligence)\b"), "0613"),
    (re.compile(r"\b(chemical engineering|process technology|laboratory|petrochemical)\b"), "0711"),
    (re.compile(r"\b(environmental engineering|environmental protection|sanitary engineering|waste|recycling)\b"), "0712"),
    (re.compile(r"\b(electrical|power|energy|air-conditioning|refrigeration|heating|solar|nuclear)\b"), "0713"),
    (re.compile(r"\b(electronic|electronics|automation|robotics|mechatronics|telecommunications|digital technology|microelectronics)\b"), "0714"),
    (re.compile(r"\b(mechanical|mechanics|metallurgy|metal|hydraulic|industrial engineering|production engineering|precision engineering|industrial maintenance)\b"), "0715"),
    (re.compile(r"\b(aerospace|aeronautical|aircraft|aviation|automotive|motor|maritime|naval|transport engineering|air transport|agricultural equipment)\b"), "0716"),
    (re.compile(r"\b(biomedical engineering|bioengineering|nanotechnology|maintenance technology)\b"), "0719"),
    (re.compile(r"\b(food|brewing|dairy|oenology|meat and poultry)\b"), "0721"),
    (re.compile(r"\b(materials?|wood|paper|plastic|polymer|glass|packaging)\b"), "0722"),
    (re.compile(r"\b(textile|garment|footwear|leather|tailoring|clothing and sewing)\b"), "0723"),
    (re.compile(r"\b(mining|petroleum|oil and gas|geological engineering|quarry)\b"), "0724"),
    (re.compile(r"\b(architecture|architectural|town planning|regional planning|rural planning|urban design|landscape architecture)\b"), "0731"),
    (re.compile(r"\b(civil engineering|construction|structural|surveying|mapping|building|road|bridge|sanitation|engineering drawing)\b"), "0732"),
    (re.compile(r"\b(agricultur(?:e|al)?|agronomy|crop|animal husbandry|animal science|farm|soil science|plant and crop protection|plant protection|apiculture|agrobiology|irrigation|harvest technology|tropical agriculture)\b"), "0811"),
    (re.compile(r"\b(horticulture|gardening|floriculture|landscape gardening|nursery)\b"), "0812"),
    (re.compile(r"\b(forestry|logging|forest products)\b"), "0821"),
    (re.compile(r"\b(fishery|fisheries|aquaculture|mariculture)\b"), "0831"),
    (re.compile(r"\b(veterinary|animal health)\b"), "0841"),
    (re.compile(r"\b(dentistry|dental|orthodontics|oral surgery|periodontics|oral pathology)\b"), "0911"),
    (re.compile(r"\b(medicine|medical science|anaesthes|surgery|orthopaed|psychiat|dermatology|gynaecology|neurology|paediatrics|forensic medicine|epidemiology|biomedicine|otorhinolaryngology|internal medicine|oncology|urology|nephrology|endocrinology|gastroenterology|rheumatology|pneumology|diabetology|hepatology|venereology|medical parasitology|public health|community health|podiatry|pre[- ]?med)\b"), "0912"),
    (re.compile(r"\b(nursing|nurses?|midwifery|elder care|gerontology)\b"), "0913"),
    (re.compile(r"\b(radiology|radiography|medical laboratory|diagnostic|audiology|prosthetic|optometry|ambulance|medical auxiliaries|prosthetics and orthotics|treatment techniques)\b"), "0914"),
    (re.compile(r"\b(therapy|rehabilitation|physio|physical therapy|occupational therapy|speech pathology|speech therapy|dietetics|nutrition|massage|mental health|art therapy|respiratory therapy|ergotherapy)\b"), "0915"),
    (re.compile(r"\b(pharmacy|pharmacology)\b"), "0916"),
    (re.compile(r"\b(acupuncture|traditional medicine|ayurvedic|homeopathic|holistic|naturopathic|herbal|yoga|chiropractic)\b"), "0917"),
    (re.compile(r"\b(child care|youth|day care)\b"), "0922"),
    (re.compile(r"\b(social work|social welfare|community services|family studies|welfare)\b"), "0923"),
    (re.compile(r"\b(beauty|hairdressing|manicure|pedicure|make up|salon)\b"), "1012"),
    (re.compile(r"\b(hospitality|hotel|restaurant|culinary|catering|cooking)\b"), "1013"),
    (re.compile(r"\b(sport|sports|physical education|coaching|fitness|gymnastics|football|jockeying|kinesiology)\b"), "1014"),
    (re.compile(r"\b(tourism|leisure|recreation|travel|guiding)\b"), "1015"),
    (re.compile(r"\b(occupational health|health and safety|industrial hygiene|stress management|ergonomics)\b"), "1022"),
    (re.compile(r"\b(policing|police|security|protective services|fire|justice administration|law enforcement)\b"), "1032"),
    (re.compile(r"\b(transport|navigation|shipping|air traffic|driving|railway|postal|nautical)\b"), "1041"),
)


def normalize_space(value: str) -> str:
    return MULTISPACE_RE.sub(" ", value or "").strip()


def _ascii_fold(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


def split_bachelor_programs(value: str) -> List[str]:
    return [part for part in (normalize_space(item) for item in (value or "").split(",")) if part]


def _truncate_degree_noise(text: str) -> str:
    lowered = _ascii_fold(text).casefold()
    markers = (
        " fields of study:",
        " offered by ",
        " associate degree",
        " advanced diploma",
        " postgraduate diploma",
        " postgraduate certificate",
        " postgraduate ",
        " post-titulo",
        " post titulo",
        " post-bachelor",
        " post bachelor",
        " especial",
        " aperfei",
        " specialisation",
        " specialization",
        " specialista",
        " specialist diploma",
        " professional ",
        " professional title",
        " professional doctorate",
        " medical doctor",
        " honours degree",
        " honors degree",
        " konzertexamen",
        " aspirantura",
        " fan doktori",
        " kandidat ",
        " magistr ",
        " magistris ",
        " mestre ",
        " mastere ",
        " mast",
        " docteur ",
        " doctor ",
        " doutor",
        " doutorado",
        " diploma ",
        " diplome ",
        " diplom ",
        " diplom-ingenieur",
        " licenc",
        " lizentiat",
        " yrkesexamen",
        " kirchliche",
        " kunstlerische",
        " lekarz",
        " mestre",
        " magistr",
        " swiadectwo",
    )
    positions = [lowered.find(marker) for marker in markers if lowered.find(marker) > 0]
    if positions:
        return text[: min(positions)]
    return text


def clean_program_title(raw_title: str) -> str:
    title = normalize_space(raw_title)
    title = re.sub(r"^\(([^)]*)\)\s+in\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^also\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^bachelor(?:\s+honors)?\s+degree\s+in:?\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^bachelor of(?: arts| science| education)?\s+in\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^bachelor of\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"^bachelor\s+", "", title, flags=re.IGNORECASE)
    title = _truncate_degree_noise(title)
    title = re.sub(r"\bnote\b.*$", "", title, flags=re.IGNORECASE)
    title = title.replace("&", " and ")
    title = re.sub(r"[()]", " ", title)
    title = normalize_space(title).strip(" ,;:-/")
    return title


def _normalize_lookup_key(value: str) -> str:
    normalized = _ascii_fold(clean_program_title(value)).casefold()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("/", " ")
    normalized = re.sub(r"[^\w\s+-]", " ", normalized)
    normalized = normalize_space(normalized)
    return normalized


def _token_lookup_key(value: str) -> str:
    normalized = _normalize_lookup_key(value)
    tokens = [token for token in re.split(r"[\s+-]+", normalized) if token and token not in STOP_WORDS]
    return " ".join(tokens)


def _raw_title_lookup_key(value: str) -> str:
    normalized = _ascii_fold(normalize_space(value)).casefold()
    normalized = normalized.replace("&", " and ")
    normalized = normalized.replace("/", " ")
    normalized = re.sub(r"[^\w\s+-]", " ", normalized)
    return normalize_space(normalized)


@lru_cache(maxsize=1)
def _load_subject_index() -> List[Dict[str, str]]:
    if not ISCED_SUBJECT_INDEX_PATH.exists():
        return []
    return json.loads(ISCED_SUBJECT_INDEX_PATH.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _build_exact_lookup() -> Dict[str, List[str]]:
    lookup: Dict[str, set[str]] = {}

    def add(key: str, code: str) -> None:
        if not key:
            return
        lookup.setdefault(key, set()).add(code)

    for entry in _load_subject_index():
        code = str(entry.get("code", "")).strip()
        title = str(entry.get("title", "")).strip()
        add(_normalize_lookup_key(title), code)
        add(_token_lookup_key(title), code)

    for code, title in FIELD_NAMES.items():
        add(_normalize_lookup_key(title), code)
        add(_token_lookup_key(title), code)

    return {key: sorted(values) for key, values in lookup.items()}


@lru_cache(maxsize=1)
def _build_exact_overrides() -> Dict[str, str]:
    result: Dict[str, str] = {}
    for title, code in RAW_EXACT_OVERRIDES.items():
        result[_normalize_lookup_key(title)] = code
        token_key = _token_lookup_key(title)
        if token_key:
            result[token_key] = code
    return result


@lru_cache(maxsize=1)
def _build_raw_title_exact_overrides() -> Dict[str, str]:
    return {
        _raw_title_lookup_key(title): code
        for title, code in RAW_TITLE_EXACT_OVERRIDES.items()
    }


def classify_bachelor_program(raw_title: str) -> str:
    raw_override = _build_raw_title_exact_overrides().get(_raw_title_lookup_key(raw_title))
    if raw_override:
        return raw_override

    cleaned = clean_program_title(raw_title)
    if not cleaned:
        return ""

    normalized = _normalize_lookup_key(cleaned)
    token_key = _token_lookup_key(cleaned)
    exact_overrides = _build_exact_overrides()
    exact_lookup = _build_exact_lookup()

    for key in (normalized, token_key):
        if key in exact_overrides:
            return exact_overrides[key]
        codes = exact_lookup.get(key, [])
        if len(codes) == 1:
            return codes[0]

    if normalized in KNOWN_LANGUAGES or token_key in KNOWN_LANGUAGES:
        return "0231"

    if normalized in LANGUAGE_GROUPS or token_key in LANGUAGE_GROUPS:
        return "0231"

    if normalized in REGIONAL_STUDIES or token_key in REGIONAL_STUDIES:
        return "0314"

    if "native language" in normalized:
        if "education" in normalized or "teacher" in normalized:
            return "0114"
        return "0232"

    if "education" in normalized or "teacher" in normalized:
        if re.search(r"pre[- ]?school|preprimary|early childhood|kindergarten", normalized):
            return "0112"
        if re.search(r"primary|elementary|special|adult|handicapped", normalized):
            return "0113"
        if re.search(r"secondary|physical education|language|vocational|subject|higher education teacher", normalized):
            return "0114"
        return "0111"

    for pattern, code in PATTERN_RULES:
        if pattern.search(normalized):
            return code

    if normalized.endswith(" studies"):
        return "0314"

    if normalized.endswith(" engineering"):
        return "0710"

    if normalized.endswith(" science") or normalized.endswith(" sciences"):
        return "0500"

    return "0000"


def classify_bachelors_cell(value: str) -> str:
    programs = split_bachelor_programs(value)
    if not programs:
        return ""
    return ", ".join(classify_bachelor_program(program) for program in programs)


def build_bachelor_program_map(programs: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for program in sorted({normalize_space(item) for item in programs if normalize_space(item)}, key=str.casefold):
        mapping[program] = classify_bachelor_program(program)
    return mapping


def write_bachelor_program_map(programs: Iterable[str], output_path: Path | None = None) -> Path:
    output_path = output_path or BACHELOR_PROGRAM_MAP_PATH
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(build_bachelor_program_map(programs), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return output_path
