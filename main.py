import csv

def extract_clave_ct(csv_file_path, output_txt_path):
    clave_ct_list = []
    
    with open(csv_file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            clave_ct = row.get('CLAVE CT', '').strip()
            if clave_ct:
                clave_ct_list.append(clave_ct)
    
    with open(output_txt_path, 'w', encoding='utf-8') as output_file:
        for clave in clave_ct_list:
            output_file.write(clave + '\n')
    
    return clave_ct_list

def main():
    csv_file = "PRIMARIA FEDERAL(PRIMARIA FEDERAL).csv"
    output_file = "clave_ct_list_federal_primaria.txt"
    
    try:
        claves = extract_clave_ct(csv_file, output_file)
        print(f"Extracted {len(claves)} CLAVE CT values to {output_file}")
    except FileNotFoundError:
        print(f"CSV file '{csv_file}' not found")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
