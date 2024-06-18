import json

def pretty_print(json_dict):
    try:
        pretty_json_str = json.dumps(json_dict, indent=4)
        print(pretty_json_str)
    except TypeError:
        print("Invalid JSON dictionary")
