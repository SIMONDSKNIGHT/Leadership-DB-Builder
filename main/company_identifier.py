import csv


JP_occupations = ['社長','上席','執行','入社','社外','入行','取締役', '取締役','取締', '部長','常任', '課長', '係長', '主任', 'リーダー', 'エンジニア', 'デザイナー', 'プログラマー','弁護人','監査役','監査' '会計士', '経営者', '営業', 'マネージャー','代表', 'コンサルタント','法人']
class CompanyIdentifier:
    def __init__(self, input=''):
        self.mode = 'normal'
        self.company_names = self.read_csv(input)
    def set_company_names(self, company_names):
        self.company_names = company_names

    def read_csv(self, csv_file):
        
        if not csv_file.endswith('.csv'):
            self.mode = 'alt'
            csv_file = self.drop_extraneous(csv_file)
            return csv_file
        company_names = {}
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            #skip the first row
            next(reader)
            for row in reader:
                names = row[1].split('|')
                company_names[row[0]] = names
        return company_names
    def identify_company(self, text):

        text = self.isolate_company_name(text)
        text = self.drop_extraneous(text)

        id = self.calculate_closest_name(text)
        return id

    def isolate_company_name(self, text):

        #given a text that contains a company name and position in japanese, try to remove all occupational titles and return the company name (which will probably be everything before the occupation name
        for occupation in JP_occupations:
            
            if occupation in text:
                return self.isolate_company_name(text.split(occupation)[0])
        return text
    def drop_extraneous(self,text):
        #drop 株式会社, 有限会社, （株）, （有）, etc
        #MUST BE USED AFTER ISOLATE COMPANY NAME, OR ELSE IT WILL DROP (現任)
        if '株式会社' in text:
            text = text.replace('株式会社','')
        if '有限会社' in text:
            text = text.replace('有限会社','')
        if '（株）' in text:
            text = text.replace('（株）','')
        if '㈱' in text:
            text = text.replace('㈱','')
        if '(現' in text:

            text = text.split('(現')[1]
        if '（現' in text:
            text = text.split('（現')[1]

        return text
    
    def calculate_closest_name(self, text):
        old=text
        if self.mode == 'alt':
            
            #compare the 1 company name in the company name string to the text
            text = self.isolate_company_name(text)
            text = self.drop_extraneous(text)

            return self.normalised_distance(text, self.company_names)
        
        lev_dist = {}
        # for each company name, calculate the levenstein distance from the 3 names in the company names dict, and get the largest one

        for company in self.company_names:
            smallest_lev = float('inf')
            for name in self.company_names[company]:
                lev = self.distance(name, text)
                
                if lev < smallest_lev:
                    smallest_lev = lev
            lev_dist[company] = smallest_lev

        #return item in lev dist with the smallest value
        return min(lev_dist, key=lev_dist.get), lev_dist[min(lev_dist, key=lev_dist.get)]


    def alt_distance(self,a,b):
        ###this one ignores 
        len_a = len(a)
        len_b = len(b)
        dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]

        for i in range(len_a + 1):
            for j in range(len_b + 1):
                if i == 0:
                    dp[i][j] = j  # All insertions
                elif j == 0:
                    dp[i][j] = 0  # Ignore deletions
                elif a[i - 1] == b[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1]
                else:
                    dp[i][j] = 1 + min(dp[i][j - 1], dp[i - 1][j - 1])

        return dp[len_a][len_b]
    def distance(self, a, b, max_distance=20):
        len_a, len_b = len(a), len(b)
        if abs(len_a - len_b) > max_distance:
            return max_distance + 1

        dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]

        for i in range(len_a + 1):
            dp[i][0] = i
        for j in range(len_b + 1):
            dp[0][j] = j

        for i in range(1, len_a + 1):
            for j in range(1, len_b + 1):
                if a[i - 1] == b[j - 1]:
                    cost = 0
                else:
                    cost = 1
                dp[i][j] = min(dp[i - 1][j] + 1,    # Deletion
                               dp[i][j - 1] + 1,    # Insertion
                               dp[i - 1][j - 1] + cost)  # Substitution

                if dp[i][j] > max_distance:
                    return max_distance + 1

        return dp[len_a][len_b]

    def normalised_distance(self, a, b):
        max_distance = 20  # Increase the max distance threshold
        max_length = max(len(a), len(b))
        if max_length == 0:
            return 0.0
        raw_distance = self.distance(a, b, max_distance)

        if raw_distance > max_distance:
            return 1.0
        return raw_distance / max_length
    # def distance(self, a, b, max_distance=float('inf')):
        
        
    #     # Calculate the Levenshtein distance between 2 strings
    #     len_a = len(a)
    #     len_b = len(b)
    #     dp = [[0] * (len_b + 1) for _ in range(len_a + 1)]

    #     for i in range(len_a + 1):
    #         for j in range(len_b + 1):
    #             if i == 0:
    #                 dp[i][j] = j
    #             elif j == 0:
    #                 dp[i][j] = i
    #             elif a[i - 1] == b[j - 1]:
    #                 dp[i][j] = dp[i - 1][j - 1]
    #             else:
    #                 dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
                
    #             # Early stopping if the current distance exceeds max_distance
    #             if dp[i][j] > max_distance:
    #                 return max_distance + 1

    # def normalised_distance(self, a, b):
    #     max_distance = 20

    #     # Normalize the Levenshtein distance
    #     max_length = max(len(a), len(b))
    #     if max_length == 0:
    #         return 0.0
    #     raw_distance = self.distance(a, b, max_distance)
    #     if raw_distance == None:
    #         return 1.0

    #     return raw_distance / max_length


# Example usage