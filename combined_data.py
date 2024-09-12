import os
import pyarrow.parquet as pq
import pyarrow as pa

def combine_parquet_files(base_dir='data', output_file='combined_data.parquet'):
    tables = []
    table_count = 0

    # Percorre todos os arquivos Parquet no diretório e subdiretórios
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                # Lê o arquivo Parquet e o adiciona à lista
                table = pq.read_table(file_path)
                tables.append(table)
                table_count += 1
                print(f"Lendo tabela {table_count}: {file_path}")


    if tables:
        combined_table = pa.concat_tables(tables)


        pq.write_table(combined_table, output_file)
        print(f"Todos os dados foram combinados e salvos em '{output_file}'")
    else:
        print("Nenhum arquivo Parquet foi encontrado para combinar.")

if __name__ == "__main__":
    combine_parquet_files()
