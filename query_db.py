import argparse
import csv
import sqlite3
import sys

def query_database(db_file, genes):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Prepare a parameterized query to select gene, image URL, stain localization, and developmental stage for multiple genes
    query = '''
        SELECT reporters.probe_gene_predicted, images.url, stainings.staining_localization, biomaterials.developmental_stage
        FROM reporters
        INNER JOIN experiments ON reporters.id = experiments.reporter_id
        INNER JOIN images ON experiments.id = images.experiment_id
        INNER JOIN stainings ON images.id = stainings.image_id
        INNER JOIN biomaterials ON experiments.id = biomaterials.experiment_id
        WHERE reporters.probe_gene_predicted IN ({})
    '''.format(','.join(['?'] * len(genes)))

    # Execute the parameterized query with the list of genes
    cursor.execute(query, genes)

    results = cursor.fetchall()

    if results:
        # Prepare CSV writer
        csv_writer = csv.writer(sys.stdout)
        csv_writer.writerow(["Gene", "Image URL", "Stain Localization", "Developmental Stage"])

        # Write results to CSV
        for result in results:
            gene, image_url, stain_localization, developmental_stage = result
            csv_writer.writerow([gene, image_url, stain_localization, developmental_stage])

    conn.close()

def main():
    # Create the command-line argument parser
    parser = argparse.ArgumentParser(description='Query SQLite database for image URL, stain localization, and developmental stage')
    parser.add_argument('db_file', type=str, help='path to the SQLite database file')
    parser.add_argument('genes', nargs='+', type=str, help='genes to query (separated by spaces)')

    # Parse the command-line arguments
    args = parser.parse_args()

    query_database(args.db_file, args.genes)

if __name__ == '__main__':
    main()
