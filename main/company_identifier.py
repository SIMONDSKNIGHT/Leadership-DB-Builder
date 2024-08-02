import csv


JP_occupations = ['社長', '取締役','取締', '部長', '課長', '係長', '主任', 'リーダー', 'エンジニア', 'デザイナー', 'プログラマー','弁護人','監査' '会計士', '経営者', '営業', 'マネージャー', 'コンサルタント']
class CompanyIdentifier:
    def __init__(self, csv_file):
        self.company_names = self.read_csv(csv_file)

    def read_csv(self, csv_file):
        company_names = {}
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                names = row[1].split('|')
                company_names[row[0]] = names
        return company_names
    def identify_company(self, text):
        text = self.isolate_company_name(text)
        id = self.calculate_closest_name(text)
        return id

    def isolate_company_name(self, text):
        #given a text that contains a company name and position in japanese, try to remove all occupational titles and return the company name (which will probably be everything before the occupation name
        for occupation in JP_occupations:
            if occupation in text:
                return text.split(occupation)[0]
            else:

                print(f'unparseable text is {text}')
                input()
                return text
            
    def calculate_closest_name(self, text):
        lev_dist = {}
        # for each company name, calculate the levenstein distance from the 3 names in the company names dict, and get the largest one

        for company in self.company_names:
            largest_lev = 0
            for name in self.company_names[company]:
                lev = self.distance(name, text)
                if lev > largest_lev:
                    largest_lev = lev
            lev_dist[company] = largest_lev

        #return max in lev_dist

        return max(lev_dist, key=lev_dist.get) 
    def alt_distance(self,a,b):
        len_a = len(a)
        len_b = len(b)
        dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]

        for i in range(len_a + 1):
            for j in range(len_b + 1):
                if i == 0:
                    dp[i][j] = j  # All insertions
                elif j == 0:
                    dp[i][j] = 0  # Ignore deletions
                elif s1[i - 1] == s2[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    dp[i][j] = 1 + min(dp[i][j - 1], dp[i - 1][j - 1])

        return dp[len_a][len_b]
    def distance(self, a, b):
        #calculate the levenshtein distance between 2 strings
        if not a: return len(b)
        if not b: return len(a)
        return min(self.distance(a[1:], b[1:])+(a[0] != b[0]), self.distance(a[1:], b)+1, self.distance(a, b[1:])+1)

# Example usage
csv_file = 'variantnames.py'
identifier = CompanyIdentifier(csv_file)
input_company_name = 'Acme Corporation'
most_likely_match = identifier.find_most_likely_match(input_company_name)
print(f"The most likely match for '{input_company_name}' is '{most_likely_match}'")