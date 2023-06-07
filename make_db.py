import argparse
import xml.etree.ElementTree as ET
import sqlite3
import os

def create_database(cursor):
    # Create the tables in the database
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS experiments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id TEXT,
            experiment_type TEXT,
            annotation_status TEXT,
            publication_status TEXT,
            information_url TEXT,
            experiment_control_id TEXT,
            author_id INTEGER,
            curator_id INTEGER,
            annotator_id INTEGER,
            source_id INTEGER,
            reporter_id INTEGER,
            author_date TEXT,
            annotation_date TEXT,
            curation_date TEXT,
            FOREIGN KEY (author_id) REFERENCES people(id),
            FOREIGN KEY (curator_id) REFERENCES people(id),
            FOREIGN KEY (annotator_id) REFERENCES people(id),
            FOREIGN KEY (source_id) REFERENCES sources(id),
            FOREIGN KEY (reporter_id) REFERENCES reporters(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sources (
            id INTEGER PRIMARY KEY,
            type TEXT,
            value TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            surname TEXT,
            mail TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS reporters (
            id INTEGER PRIMARY KEY,
            probe_id TEXT,
            probe_id_source TEXT,
            probe_type TEXT,
            probe_gene_predicted TEXT,
            probe_protocols TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS biomaterials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER,
            species_name TEXT,
            taxon_id TEXT,
            taxon_id_source TEXT,
            provider_specimen TEXT,
            developmental_stage TEXT,
            developmental_stage_source TEXT,
            phenotype TEXT,
            FOREIGN KEY (experiment_id) REFERENCES experiments(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS treatments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            biomaterial_id INTEGER,
            treatment TEXT,
            FOREIGN KEY (biomaterial_id) REFERENCES biomaterials(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            experiment_id INTEGER,
            url TEXT,
            note TEXT,
            extra_info TEXT,
            subcell_pos TEXT,
            FOREIGN KEY (experiment_id) REFERENCES experiments(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stainings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            image_id INTEGER,
            staining_localization TEXT,
            localization_source TEXT,
            staining_detection_method TEXT,
            FOREIGN KEY (image_id) REFERENCES images(id)
        )
    ''')

def parse_xml(cursor, root):
    # Lookup dictionary for people IDs
    people_lookup = {}

    # Lookup dictionary for source IDs
    source_lookup = {}

    # Lookup dictionary for reporter IDs
    reporter_lookup = {}

    # Iterate over each experiment element
    for experiment in root.findall('experiment'):
        experiment_id = experiment.get('id')
        experiment_type = get_text(experiment, 'experiment_design/type')
        annotation_status = get_text(
            experiment, 'experiment_design/annotation_status')
        publication_status = get_text(
            experiment, 'experiment_design/publication_status')
        information_url = get_text(
            experiment, 'experiment_design/information_url')
        experiment_control_id = get_text(
            experiment, 'experiment_design/experiment_control_id')

        # Insert experiment data into the experiments table
        cursor.execute('''
            INSERT INTO experiments
            (experiment_id, experiment_type, annotation_status, publication_status, information_url, experiment_control_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (experiment_id, experiment_type, annotation_status, publication_status, information_url, experiment_control_id))

        experiment_rowid = cursor.lastrowid

        # Process source information
        try:
            source_element = experiment.find('experiment_design/source/*')

            if source_element is not None:
                source_type = source_element.tag
                source_value = source_element.text

                # Check if the source already exists in the lookup dictionary
                source_id = source_lookup.get((source_type, source_value))

                if source_id is None:
                    # Insert source data into the sources table
                    cursor.execute('''
                        INSERT INTO sources
                        (type, value)
                        VALUES (?, ?)
                    ''', (source_type, source_value))

                    source_id = cursor.lastrowid
                    source_lookup[(source_type, source_value)] = source_id

                # Update source_id in the experiments table
                cursor.execute('''
                    UPDATE experiments SET source_id = ? WHERE id = ?
                ''', (source_id, experiment_rowid))
        except AttributeError:
            pass

        # Process author information
        try:
            author_element = experiment.find(
                'experiment_design/contact_information/author')
            author_name = get_text(author_element, 'author_name')
            author_surname = get_text(author_element, 'author_surname')
            author_mail = get_text(author_element, 'author_mail')
            author_date = get_text(author_element, 'author_date')

            # Get author ID from the lookup dictionary or insert a new record
            author_id = people_lookup.get(
                (author_name, author_surname, author_mail))
            if author_id is None:
                cursor.execute('''
                    INSERT INTO people (name, surname, mail)
                    VALUES (?, ?, ?)
                ''', (author_name, author_surname, author_mail))
                author_id = cursor.lastrowid
                people_lookup[(author_name, author_surname,
                               author_mail)] = author_id

            # Update author_id in the experiments table
            cursor.execute('''
                UPDATE experiments SET author_id = ?, author_date = ? WHERE id = ?
            ''', (author_id, author_date, experiment_rowid))

        except AttributeError:
            pass

        # Process annotator information
        try:
            annotator_element = experiment.find(
                'experiment_design/contact_information/annotator')
            annotator_name = get_text(annotator_element, 'annotator_name')
            annotator_surname = get_text(
                annotator_element, 'annotator_surname')
            annotator_mail = get_text(annotator_element, 'annotator_mail')
            annotation_date = get_text(annotator_element, 'annotation_date')

            # Get annotator ID from the lookup dictionary or insert a new record
            annotator_id = people_lookup.get(
                (annotator_name, annotator_surname, annotator_mail))
            if annotator_id is None:
                cursor.execute('''
                    INSERT INTO people (name, surname, mail)
                    VALUES (?, ?, ?)
                ''', (annotator_name, annotator_surname, annotator_mail))
                annotator_id = cursor.lastrowid
                people_lookup[(annotator_name, annotator_surname,
                               annotator_mail)] = annotator_id

            # Update annotator_id in the experiments table
            cursor.execute('''
                UPDATE experiments SET annotator_id = ?, annotation_date = ? WHERE id = ?
            ''', (annotator_id, annotation_date, experiment_rowid))

        except AttributeError:
            pass

        # Process curator information
        try:
            curator_element = experiment.find(
                'experiment_design/contact_information/curator')
            curator_name = get_text(curator_element, 'curator_name')
            curator_surname = get_text(curator_element, 'curator_surname')
            curator_mail = get_text(curator_element, 'curator_mail')
            curation_date = get_text(curator_element, 'curation_date')

            # Get curator ID from the lookup dictionary or insert a new record
            curator_id = people_lookup.get(
                (curator_name, curator_surname, curator_mail))
            if curator_id is None:
                cursor.execute('''
                    INSERT INTO people (name, surname, mail)
                    VALUES (?, ?, ?)
                ''', (curator_name, curator_surname, curator_mail))
                curator_id = cursor.lastrowid
                people_lookup[(curator_name, curator_surname,
                               curator_mail)] = curator_id

            # Update curator_id in the experiments table
            cursor.execute('''
                UPDATE experiments SET curator_id = ?, curation_date = ? WHERE id = ?
            ''', (curator_id, curation_date, experiment_rowid))

        except AttributeError:
            pass

        # Process biomaterial information
        try:
            biomaterial_element = experiment.find(
                'biomaterial_treatments/biomaterial')
            species_name = get_text(biomaterial_element, 'species_name')
            taxon_id = get_text(biomaterial_element, 'taxon_id')
            taxon_id_source = get_text(biomaterial_element, 'taxon_id_source')
            provider_specimen = get_text(
                biomaterial_element, 'provider_specimen')
            developmental_stage = get_text(
                biomaterial_element, 'developmental_stage')
            developmental_stage_source = get_text(
                biomaterial_element, 'developmental_stage_source')
            phenotype = get_text(biomaterial_element, 'phenotype')

            # Insert biomaterial data into the biomaterials table
            cursor.execute('''
                INSERT INTO biomaterials
                (experiment_id, species_name, taxon_id, taxon_id_source, provider_specimen, developmental_stage,
                 developmental_stage_source, phenotype)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (experiment_rowid, species_name, taxon_id, taxon_id_source, provider_specimen, developmental_stage,
                  developmental_stage_source, phenotype))

            biomaterial_rowid = cursor.lastrowid

            # Process biomaterial treatments
            treatments_element = biomaterial_element.find('treatments')
            try:
                treatments = treatments_element.findall('treatment')

                for treatment in treatments:
                    treatment_text = treatment.text

                    # Insert treatment data into the treatments table
                    cursor.execute('''
                        INSERT INTO treatments
                        (biomaterial_id, treatment)
                        VALUES (?, ?)
                    ''', (biomaterial_rowid, treatment_text))
            except AttributeError:
                pass
        except AttributeError:
            pass

        # Process reporter information
        try:
            reporter_element = experiment.find('expression/reporter')
            probe_id = get_text(reporter_element, 'probe_id')
            probe_id_source = get_text(reporter_element, 'probe_id_source')
            probe_type = get_text(reporter_element, 'probe_type')
            probe_gene_predicted = get_text(
                reporter_element, 'probe_gene_predicted')
            probe_protocols = get_text(reporter_element, 'probe_protocols')

            # Check if the reporter already exists in the lookup dictionary
            reporter_id = reporter_lookup.get((probe_id, probe_id_source, probe_type, probe_gene_predicted, probe_protocols))

            if reporter_id is None:
                # Insert reporter data into the reporters table
                cursor.execute('''
                    INSERT INTO reporters
                    (probe_id, probe_id_source, probe_type, probe_gene_predicted, probe_protocols)
                    VALUES (?, ?, ?, ?, ?)
                ''', (probe_id, probe_id_source, probe_type, probe_gene_predicted, probe_protocols))

                reporter_id = cursor.lastrowid
                reporter_lookup[(probe_id, probe_id_source, probe_type, probe_gene_predicted, probe_protocols)] = reporter_id

            # Update reporter_id in the experiments table
            cursor.execute('''
                UPDATE experiments SET reporter_id = ? WHERE id = ?
            ''', (reporter_id, experiment_rowid))
        except AttributeError:
            pass

        # Process image information
        try:
            image_url = experiment.find('expression/imaging/image_data/image').attrib['url']

            # Process image note
            images_element = experiment.find('expression/images')
            image_note = get_text(images_element, 'image_note')
            image_extra_info = get_text(images_element, 'image_characterization/extra_info')
            image_subcell_pos = get_text(images_element, 'image_characterization/subcell_pos')

            # Insert image data into the images table
            cursor.execute('''
                INSERT INTO images
                (experiment_id, url, note, extra_info, subcell_pos)
                VALUES (?, ?, ?, ?, ?)
            ''', (experiment_rowid, image_url, image_note, image_extra_info, image_subcell_pos))

            image_rowid = cursor.lastrowid

            # Process staining information
            try:
                staining_localization = get_text(
                    images_element, 'image_characterization/staining_localization')
                localization_source = get_text(
                    images_element, 'image_characterization/localization_source')
                staining_detection_method = get_text(
                    experiment, 'expression/staining/staining_detection_method')

                # Insert staining data into the stainings table
                cursor.execute('''
                    INSERT INTO stainings
                    (image_id, staining_localization, localization_source, staining_detection_method)
                    VALUES (?, ?, ?, ?)
                ''', (image_rowid, staining_localization, localization_source, staining_detection_method))
            except AttributeError:
                pass
        except AttributeError:
            pass


def get_text(parent_element, tag):
    element = parent_element.find(tag)
    if element is not None:
        return element.text
    return None


def main():
    # Create the command-line argument parser
    parser = argparse.ArgumentParser(
        description='Serialize XML data to a SQLite database')
    parser.add_argument('xml_file', type=str, help='path to the XML file')
    parser.add_argument('db_file', type=str,
                        help='path to the SQLite database file')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite the existing database file')

    # Parse the command-line arguments
    args = parser.parse_args()

    # Check if the database file already exists
    if os.path.isfile(args.db_file) and args.overwrite:
        # Remove the old database file
        os.remove(args.db_file)
    elif os.path.isfile(args.db_file) and not args.overwrite:
        print(
            f"Database file '{args.db_file}' already exists. Use the '-o' option to overwrite.")
        return

    # Create a new SQLite database connection
    conn = sqlite3.connect(args.db_file)
    cursor = conn.cursor()

    try:
        create_database(cursor)
        tree = ET.parse(args.xml_file)
        root = tree.getroot()
        parse_xml(cursor, root)
        conn.commit()
        print(
            f"Successfully serialized XML data to the database file: {args.db_file}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
