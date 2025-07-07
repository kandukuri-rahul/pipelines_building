from src.insertion import extract_from_text,load_to_mongoDB
from src.extract import read_data,normalize_technologies
from src.load import load_to_ssms
records = extract_from_text(r'C:\Users\Rahul\Documents\Visual Studio 2017\python\mongodb\src\project (1).txt')
data = load_to_mongoDB(records)
fetched_data = read_data()
fetched_data = normalize_technologies(fetched_data)
load_to_ssms(fetched_data)
