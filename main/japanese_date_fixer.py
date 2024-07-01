
def convert_to_western_date(japanese_date):
    # Define the era start years and corresponding era names
    era_start_years = [1868, 1912, 1926, 1989]
    era_names = ['明治', '大正', '昭和', '平成']

    # Split the Japanese date into era and year
    era, year = japanese_date.split(' ')

    # Find the corresponding era start year
    era_start_year = None
    for i in range(len(era_start_years)):
        if era == era_names[i]:
            era_start_year = era_start_years[i]
            break

    # If era start year is found, convert the date to western format
    if era_start_year:
        western_year = era_start_year + int(year) - 1
        return f'{western_year}-01-01'
    else:
        return None

# Example usage
japanese_date = 'Heisei 31'
western_date = convert_to_western_date(japanese_date)
print(western_date)