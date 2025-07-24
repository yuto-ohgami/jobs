from flask import Flask, request
from utilities.load import load_sheet_API, load_json_flatten_level1, bucket
from utilities.update import apply_dict_update_jquants_slice, apply_dict_update_jquants_codes_sync, apply_dict_update_jquants_codes_sync_fs_details

def run_jquants_update():
    
    # slice
    jquants_dict_path = load_sheet_API("docs_API_jquants", "slice")
    apply_dict_update_jquants_slice(jquants_dict_path)
    
    company_codes = load_json_flatten_level1("jquants_info.json")['Code'].tolist() #company_codes = ['56210', '58920']
    # codes
    jquants_dict_path_codes = load_sheet_API("docs_API_jquants", "codes")
    apply_dict_update_jquants_codes_sync(company_codes, jquants_dict_path_codes)
    
    # codes_fs_details
    jquants_dict_path_codes_fs_details = load_sheet_API("docs_API_jquants", "codes_fs_details")
    apply_dict_update_jquants_codes_sync_fs_details(company_codes, jquants_dict_path_codes_fs_details)

    return "J-Quants update completed successfully"


import requests
from utilities.api_jquants import build_headers_jquants
if __name__ == "__main__":
    try:
        headers_jquants = build_headers_jquants()
        r = requests.get("https://api.jquants.com/v1/listed/info", headers=headers_jquants)
        if r.status_code == 429:
            print("Rate limit exceeded. Please try again later.")
        else:
            run_jquants_update()
    except Exception as e:
        print(f"Error occurred during rate limit check: {e}")

