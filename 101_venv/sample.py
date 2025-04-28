import pandas as pd

def main():
    data = {
        'Name'  : ['Alice',     'Bob',          'Charlie',  'David',    'Eve'],
        'Age'   : [24,          27,             22,         32,         29],
        'City'  : ['New York',  'Los Angeles',  'Chicago',  'Houston',  'Phoenix']
    }

    df = pd.DataFrame(data)

    print("Show DataFrame:")
    print(df,"\n")

    average_age = df['Age'].mean()
    print("Average Age:")
    print(average_age,"\n")

    grouped_by_city = df.groupby('City').size()
    print("Grouped by City:")
    print(grouped_by_city,"\n")

if __name__=='__main__':
    main()
