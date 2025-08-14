"""
Define Reader Options for Products Input Data
"""
CSV_OPTIONS: dict= {
    "header": "true",
    "delimiter": ",",
    "quote": "\"",
    "escape": "\"",
    "mode": "PERMISSIVE"
}

JSON_OPTIONS: dict = {
    "multiline": "true",
    "mode": "PERMISSIVE",
}

EXCEL_OPTIONS: dict = {
    "sheetName": "Worksheet",
}
