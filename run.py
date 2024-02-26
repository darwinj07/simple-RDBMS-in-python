from lark import Lark, Transformer, UnexpectedInput, Token, Tree
from berkeleydb import db
import json
import os
import sys


with open('grammar.lark') as file:
    sql_parser=Lark(file.read(),start="command",lexer='basic') #use sql_parser to parse input into Tree

os.chdir('./DB')

metaDataDB=db.DB()
# initialize metaDataDB 
# metaDataDB.open('metaDataDB.db', dbtype=db.DB_HASH, flags=db.DB_CREATE)

metaDataDB.open('metaDataDB.db', dbtype=db.DB_HASH) #open existing metaDataDB.db file
# # myDB.close()





class CharLengthError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Char length should be over 0"

class SelectTableExistenceError(Exception):
    def __init__(self, table_name):
        self.table_name=table_name
        self.message=f"DB_2020-14094> Selection has failed: {table_name} does not exist"

class SelectColumnResolveError(Exception):
    def __init__(self, column_name):
        self.column_name=column_name
        self.message=f"DB_2020-14094> Selection has failed: fail to resolve {column_name}"
class WhereIncomparableError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Where clause trying to compare incomparable values"

class WhereTableNotSpecifiedError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Where clause trying to reference tables which are not specified"
class WhereColumnNotExistError(Exception):
    def __init__(self):
        self.message=f"DB_2020-14094> Where clause trying to reference non existing column"

class WhereAmbiguousReference(Exception):
    def __init__(self):
        self.message=f"DB_2020-14094> Where clause contains ambiguous reference"

class NoSuchTable(Exception):
     def __init__(self):
        self.message="DB_2020-14094> No such table"

class DropReferencedTableError(Exception):
    def __init__(self, table_name):
        self.table_name=table_name
        self.message=f"DB_2020-14094> Drop table has failed: {table_name} is referenced by other table"


class DuplicateColumnDefError(Exception):
     def __init__(self):
        self.message="DB_2020-14094> Create table has failed: column definition is duplicated"

class DuplicatePrimaryKeyDefError(Exception):
    def __init__(self):
      self.message="DB_2020-14094> Create table has failed: primary key definition is duplicated"


class ReferenceTypeError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Create table has failed: foreign key references wrong type"

class ReferenceNonPrimaryKeyError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Create table has failed: foreign key references non primary key column"

class ReferenceColumnExistenceError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Create table has failed: foreign key references non existing column"


class ReferenceTableExistenceError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Create table has failed: foreign key references non existing table"


class NonExistingColumnDefError(Exception):
    def __init__(self, col_name):
        self.col_name = col_name
        self.message=f"DB_2020-14094> Create table has failed: {col_name} does not exist in column definition"

class TableExistenceError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Create table has failed: table with the same name already exists"


class CharLengthError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Char length should be over 0"

    
class InsertTypeMismatchError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Insertion has failed: Types are not matched"

class InsertColumnExistenceError(Exception):
    def __init__(self, col_name):
        self.message=f"DB_2020-14094> Insertion has failed: {col_name} does not exist"
class InsertColumnNonNullableError(Exception):
    def __init__(self, col_name):
        self.message=f"DB_2020-14094> Insertion has failed: {col_name} is not nullable"
class InsertDuplicatePrimaryKeyError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Insertion has failed: Primary key duplication"
class InsertReferentialIntegrityError(Exception):
    def __init__(self):
        self.message="DB_2020-14094> Insertion has failed: Referential integrity violation"
class DeleteReferentialIntegrityError(Exception):
    def __init__(self, count):
        self.message=f"DB_2020-14094> {count} row(s) are not deleted due to referential integrity"




class MyTransformer(Transformer): #executes the corresponding action for each type of query
    def create_table_query(self, items):
        
        newSchemaDB=db.DB() #create bdb instance for new schema


        #fetch table name from parsed result
        table_name = items[2].children[0].lower()

            #check if table_name is already in metaDataDB
        if metaDataDB.get(table_name.encode('utf-8')):
            raise TableExistenceError()

        # data structures to hold extracted information
        columns = []
        primary_constraints = []
        foreign_constraints = []
        not_null_constraints = []

        column_names = set()  # to check for duplicate column names

        # go through table elements and extract column and constraint info
        for column_def in items[3].find_data("column_definition"):

            # Check for duplicate column names
            column_name = column_def.children[0].children[0].lower()
            if column_name in column_names:
                raise DuplicateColumnDefError()
            column_names.add(column_name)

            data_type_node = column_def.children[1]
            data_type_str = ''.join(str(child) for child in data_type_node.children)
            if data_type_str.startswith("char(") and data_type_str.endswith(")"):
                try:
                    char_length = int(data_type_str[5:-1])  # Extracting the value of n from "char(n)"
                    if char_length < 1:
                        raise CharLengthError()
                except ValueError:
                    raise CharLengthError()

            column_info = {
                "name": column_name,
                "type": data_type_str.lower()
            }
                            
            if column_def.children[2]!=None:  # NOT NULL is included
                not_null_constraints.append(column_name)
            columns.append(column_info)

        column_name_type_dictionary = {col["name"]: col["type"] for col in columns}

        primary_key_count = 0#to check for multiple primary keys

        # Extract primary key constraints
        for pk_constraint in items[3].find_data("primary_key_constraint"):
            # raise error if multiple primary key constraints are found
            primary_key_count += 1
            if primary_key_count > 1:
                raise DuplicatePrimaryKeyDefError()
            
            # check if primary key columns exist in the table
            for col in pk_constraint.find_data("column_name"):
                if col.children[0].lower() not in column_name_type_dictionary:
                    raise NonExistingColumnDefError(col.children[0].lower())
                primary_constraints.append(col.children[0].lower())
                if col.children[0].lower() not in not_null_constraints:
                    not_null_constraints.append(col.children[0].lower())

        # Extract referential constraints
        for ref_constraint in items[3].find_data("referential_constraint"):
            refing_col_list=[]
            refed_col_list=[]

            refing_col_list.extend([col.children[0].lower() for col in ref_constraint.children[2].find_data("column_name")])
            refed_col_list.extend([col.children[0].lower() for col in ref_constraint.children[5].find_data("column_name")])
            
            refed_table = ref_constraint.children[4].children[0].lower()
            

            refed_table_metadata_encoded = metaDataDB.get(refed_table.encode('utf-8'))

            if refed_table_metadata_encoded:
                refed_table_metadata = json.loads(refed_table_metadata_encoded.decode('utf-8'))
                    
                # Check for type match and existence.
                parent_table_column_name_type_dictionary = {col["name"]: col["type"] for col in refed_table_metadata["columns"]}
                for i in range(len(refing_col_list)):
                    refing_col=refing_col_list[i]
                    refed_col=refed_col_list[i]
                    if refing_col not in column_name_type_dictionary:
                        raise NonExistingColumnDefError(refing_col)
                    parent_column_type=parent_table_column_name_type_dictionary.get(refed_col,-1)
                    if(parent_column_type ==-1): 
                        raise ReferenceColumnExistenceError()
                    if parent_column_type != column_name_type_dictionary[refing_col]:
                        raise ReferenceTypeError()
                
                # Check if referenced columns form primary key
                if set(refed_col_list) != set(refed_table_metadata.get("primary_constraints",[])):
                    raise ReferenceNonPrimaryKeyError()
                
            else:
                # If there's no metadata available for the referenced table, it means the table does not exist
                raise ReferenceTableExistenceError()

            foreign_constraints.append({
            "refing_col_list": refing_col_list,
            "refed_table": ref_constraint.children[4].children[0].lower(),
            "refed_col_list": refed_col_list
            })



        # Check for char length if defined in columns
        for col in columns:
            if col["type"].startswith("char") and int(col["type"].split("(")[1].split(")")[0]) < 1:
                raise CharLengthError()
            

        # build metadata


        metadata = {
            "columns": list(columns) if isinstance(columns, set) else columns,
            "primary_constraints": list(primary_constraints) if isinstance(primary_constraints, set) else primary_constraints,
            "foreign_constraints": list(foreign_constraints) if isinstance(foreign_constraints, set) else foreign_constraints,
            "not_null_constraints": list(not_null_constraints) if isinstance(not_null_constraints, set) else not_null_constraints,
            "record_key": "0"
        }
        # put the metadata into the metadataDB
        metaDataDB.put(table_name.encode('utf-8'), json.dumps(metadata).encode('utf-8'))

        
        
        
        newSchemaDB.open(table_name+'.db', dbtype=db.DB_HASH, flags=db.DB_CREATE) # create bdb file for new schema
        newSchemaDB.close()



        print(f"DB_2020-14094> {table_name} table is created")
    
    
       
        

    def drop_table_query(self, items):
        
        # Fetch the table name from the parsed result.
        table_name = items[2].children[0].lower()
        
        # Remove the table's metadata from metaDataDB.
        if not metaDataDB.exists(table_name.encode('utf-8')):
            raise NoSuchTable()
        
        for _, metadata in metaDataDB.items():
            parsed_metadata = json.loads(metadata.decode('utf-8'))
            for foreign_constraint in parsed_metadata.get("foreign_constraints", []):
                if foreign_constraint["refed_table"] == table_name:
                    raise DropReferencedTableError(table_name)
        # Delete the Berkeley DB file associated with the table.
        
        # Delete the metadata associated with the table from metaDataDB.
        metaDataDB.delete(table_name.encode('utf-8'))

        db_to_drop = db.DB()
        db_to_drop.remove(filename=f"{table_name}.db")
    
        print(f"DB_2020-14094> {table_name} table is dropped.")

        
    def explain_query(self, items):
        # Fetch the table name from parsed result.
        table_name = items[1].children[0].lower()
        #call explain_describe_desc_query with table_name
        self.explain_describe_desc_query(table_name)
        
    def describe_query(self, items):
        # Fetch the table name from parsed result.
        table_name = items[1].children[0].lower()
        #call explain_describe_desc_query with table_name
        self.explain_describe_desc_query(table_name)

    def desc_query(self, items):
        # Fetch the table name from parsed result.
        table_name = items[1].children[0].lower()
        #call explain_describe_desc_query with table_name
        self.explain_describe_desc_query(table_name)
        

    def insert_query(self, items):
        # Fetch table name from parsed result
        table_name = items[2].children[0].lower()

        table_metaData_encoded=metaDataDB.get(table_name.encode('utf-8'))


        #check if table_name is in metaDataDB
        if not table_metaData_encoded:
            raise NoSuchTable()

          
        
        # Open the corresponding table from Berkeley DB
        tableDB = db.DB()
       

        tableDB.open(table_name+'.db', dbtype=db.DB_HASH)

        # Fetch metadata for the table from metaDataDB
        metadata = json.loads(table_metaData_encoded.decode('utf-8'))
         # Extract column info from the metadata
        all_columns = {col["name"]: col["type"] for col in metadata["columns"]}
        


        # extract {column_name : type} pairs to insert as dictionary. type is char(n), date or int
        inserted_columns = {}
        if items[3] is not None: #check if column_list is given
            for col in items[3].find_data("column_name"):
                col_name = col.children[0].lower()
                try:
                    inserted_columns[col_name] = all_columns[col_name]
                except KeyError:
                    raise InsertColumnExistenceError(col_name)
        else:
            # If column names are not provided, fetch them from the metaDataDB 
            inserted_columns = all_columns


        #Check if all non nullable columns are being inserted
        for col_name in metadata["not_null_constraints"]:
            if col_name not in inserted_columns:
                raise InsertColumnNonNullableError(col_name)

        # Extract (value, type) pairs to insert as list. type is str, date, int or null
        values = [(val.children[0],val.children[0].type.lower()) for val in items[5].find_data("inserted_value")]

        # Check if the number of values matches the number of columns
        if len(inserted_columns) != len(values):
            raise InsertTypeMismatchError()
        
        

        # Construct inserted_record dictionary of {column_name: value} pairs
        
        inserted_record={}
        i=-1
        for col_name, col_type in inserted_columns.items():
            i+=1
            value_type = values[i][1]
            

            # Check if the types of values match the types of columns
            if(value_type=="null"):
                if(col_name in metadata["not_null_constraints"]):
                    raise InsertColumnNonNullableError(col_name)
                continue
            else:
                if(value_type!=col_type):
                    if(not (value_type=="str" and col_type.startswith("char(") and col_type.endswith(")"))):
                       
                        raise InsertTypeMismatchError()
                    #type is char
                    else: 
                    # Truncate char values if their length exceeds n
                        n = int(col_type[5:-1])
                
                        inserted_value=values[i][0][1:min(n+1,len(values[i][0])-1)]

                else:
                    inserted_value=values[i][0]
            inserted_record[col_name]=inserted_value

        
        #Check if primary key is duplicated
        primary_constraints=metadata["primary_constraints"]
        if(primary_constraints): #if table has primary key
            inserted_primary_key_value_dictionary={col_name:inserted_record.get(col_name, -1) for col_name in metadata["primary_constraints"]}
            cursor=tableDB.cursor()
            rec=cursor.first()
            while rec:
                record = json.loads(rec[1].decode('utf-8'))
                primary_key_value_dictionary={col_name:record[col_name] for col_name in metadata["primary_constraints"]}
                if(inserted_primary_key_value_dictionary==primary_key_value_dictionary):
                    raise InsertDuplicatePrimaryKeyError()
                rec=cursor.next()
            cursor.close()
        
        #Check if record being referenced exists in referenced table
        for foreign_constraint in metadata["foreign_constraints"]:
            refing_col_list=foreign_constraint["refing_col_list"]

            #foreign keys not inserted
            if(refing_col_list[0] not in inserted_record):
                for refing_col in refing_col_list:
                    if(refing_col in inserted_record):
                        raise InsertReferentialIntegrityError()
                    
            #foreign keys inserted
            else:
                inserted_foreign_key_value_pairs={refing_col:inserted_record.get(refing_col,-1) for refing_col in refing_col_list}

                refed_col_list=foreign_constraint["refed_col_list"]
                refed_table = foreign_constraint["refed_table"]
                refed_table_db = db.DB()
                refed_table_db.open(refed_table+'.db', dbtype=db.DB_HASH)
                cursor = refed_table_db.cursor()
                rec = cursor.first()
                while rec:
                    record = json.loads(rec[1].decode('utf-8'))
                    refed_foreign_key_value_pairs={refed_col:record.get(refed_col,-1) for refed_col in refed_col_list}
                    matched=True
                    for i in range(len(refed_col_list)):
                        if(inserted_foreign_key_value_pairs[refing_col_list[i]]!=refed_foreign_key_value_pairs[refed_col_list[i]]):
                            matched=False
                            break
                    if(matched): break

                    rec = cursor.next()
                cursor.close()
                refed_table_db.close()
                if not rec:
                    raise InsertReferentialIntegrityError()
                
            

        #ensure key for every record is unique
        key=metadata["record_key"].encode('utf-8')
        

        
        # Serialize the record as JSON and store it in the table DB
        #json has no attribute for columns with null values
        tableDB.put(key, json.dumps(inserted_record).encode('utf-8'))

        next_key=str(int(key.decode('utf-8'))+1)

        metadata["record_key"]=next_key #update key for next record
   
        metaDataDB.put(table_name.encode('utf-8'), json.dumps(metadata).encode('utf-8'))

        print(f"DB_2020-14094> 1 row inserted")
        tableDB.close()



        
    def delete_query(self, items):
        # Fetch table name from parsed result
        table_name=items[2].children[0].lower()
        #check if table_name is in metaDataDB
        if not metaDataDB.get(table_name.encode('utf-8')):
            raise NoSuchTable()
        table=db.DB()
        table.open(table_name+'.db', dbtype=db.DB_HASH)

        keys_to_delete=[1]

        if(items[3]==None): #no where clause
            count=len(table)
            cursor=table.cursor()
            rec=cursor.first()
            while rec:

                
                self.delete_key_appender(keys_to_delete, table_name, cursor)
                rec=cursor.next()
            if(not keys_to_delete[0]):
                raise DeleteReferentialIntegrityError(count)
            for i in range(1, len(keys_to_delete)):
                table.delete(keys_to_delete[i])
            cursor.close()
            table.close()
            print(f"DB_2020-14094> {count} row(s) deleted")
            return
        
        #where clause exists

        # Fetch metadata for the table from metaDataDB
        metadata = json.loads(metaDataDB.get(table_name.encode('utf-8')).decode('utf-8'))
        # Extract column info from the metadata
        column_name_type_dictionary = {col["name"]: col["type"] for col in metadata["columns"]}
        column_name_type_dictionary=self.append_table_name_to_column_name(table_name, column_name_type_dictionary)
        # Fetch all records from the table DB and delete them if they satisfy where clause
        cursor = table.cursor()
        rec = cursor.first()
        count=0
        #check syntax of where clause when table is empty using fake record
        fake_record={}
        self.validate_record_by_where_clause(fake_record, items[3], column_name_type_dictionary)

        while rec:
            record = json.loads(rec[1].decode('utf-8'))
            record=self.append_table_name_to_column_name(table_name, record)
            if self.validate_record_by_where_clause(record, items[3], column_name_type_dictionary):
                self.delete_key_appender(keys_to_delete, table_name, cursor)
            rec = cursor.next()

        count=len(keys_to_delete)-1

        if(not keys_to_delete[0]):
            raise DeleteReferentialIntegrityError(count)
        for i in range(1, len(keys_to_delete)):
            table.delete(keys_to_delete[i])

        cursor.close()
        table.close()
        print(f"DB_2020-14094> {count} row(s) deleted")

    #helper function to delete records using cursor
    def delete_key_appender(self, keys_to_delete, parent_table_name, cursor):
        parent_rec=cursor.current()
        keys_to_delete.append(parent_rec[0])

        parent_record=json.loads(parent_rec[1].decode('utf-8'))
        parent_table_metaData=metaDataDB.get(parent_table_name.encode('utf-8'))
        parent_table_primary_keys=json.loads(parent_table_metaData.decode('utf-8'))["primary_constraints"]

        #check if record is referenced by record in other table and delete
        metaData_cursor=metaDataDB.cursor()
        metaData_rec=metaData_cursor.first()
        while metaData_rec: 
            child_table_name=metaData_rec[0].decode('utf-8')
            if(child_table_name!=parent_table_name): 
                child_table_metaData =json.loads(metaData_rec[1].decode('utf-8'))
                child_table_foreign_constraints=child_table_metaData["foreign_constraints"]
                for foreign_constraint in child_table_foreign_constraints:
                    refed_col_list=foreign_constraint["refed_col_list"]
                    if(parent_table_name==foreign_constraint["refed_table"] and set(parent_table_primary_keys)==set(refed_col_list)):
                        refing_col_list=foreign_constraint["refing_col_list"]
                        child_table=db.DB()
                        child_table.open(child_table_name+'.db', dbtype=db.DB_HASH)
                        child_table_cursor=child_table.cursor()
                        child_rec=child_table_cursor.first()
                        while child_rec:
                            child_record=json.loads(child_rec[1].decode('utf-8'))
                            if([child_record.get(foreign_key,-1) for foreign_key in refing_col_list]==[parent_record.get(primary_key,-1) for primary_key in refed_col_list]):
                                keys_to_delete[0]=0  
                            child_rec=child_table_cursor.next()
                        child_table_cursor.close()
                        child_table.close()
            metaData_rec=metaData_cursor.next()

   
        
    def select_query(self, items):
        record_list=[{}]
        column_name_type_dictionary={}

        #fill in column_name_type_dictionary and record_list

        for referred_table in items[2].find_data("referred_table"):
            table_name=referred_table.children[0].children[0].lower()
            table_alias=table_name
            if(referred_table.children[1]!=None):
                table_alias=referred_table.children[2].children[0].lower()
            #check if table_name is in metaDataDB
            if not metaDataDB.get(table_name.encode('utf-8')):
                raise SelectTableExistenceError(table_name)
            # Open the corresponding table from Berkeley DB
            tableDB = db.DB()
            tableDB.open(table_name+'.db', dbtype=db.DB_HASH)
            # Fetch metadata for the table from metaDataDB
            metadata = json.loads(metaDataDB.get(table_name.encode('utf-8')).decode('utf-8'))
            # Extract column info from the metadata and add to column_name_type_dictionary
            column_name_type_dictionary.update({table_alias+'.'+col["name"]: col["type"] for col in metadata["columns"]})
            # replace record_list with list of all combinations of concatenation of table records and existing records
            new_record_list=[]
            for record in record_list:
                cursor = tableDB.cursor()
                rec = cursor.first()
                while rec:
                    new_record = json.loads(rec[1].decode('utf-8'))
                    new_record=self.append_table_name_to_column_name(table_alias, new_record)
                    new_record_list.append({**record, **new_record})
                    rec = cursor.next()
                cursor.close()
            record_list=new_record_list

            tableDB.close()
        
        #fill in filtered_record_list
        where_clause=items[2].children[1]
        if(where_clause==None):
            filtered_record_list=record_list

        else:
            filtered_record_list=[]

            #check syntax of where clause when table is empty using fake record
            fake_record={}
            self.validate_record_by_where_clause(fake_record, where_clause, column_name_type_dictionary)
    
            for record in record_list:
                if(self.validate_record_by_where_clause(record, where_clause, column_name_type_dictionary)):
                    filtered_record_list.append(record)
            
        #fill in projected_columns_dictionary
        projected_columns_dictionary={}
        select_list=items[1]


        #select all columns 
        if(not select_list.children):
            projected_columns_dictionary={key:key for key in column_name_type_dictionary}
        #select specific columns
        else:
            for selected_column in select_list.find_data("selected_column"):
                
                #extract column name
                try:
                    column_name=self.extract_column_name_given_table_column(selected_column, column_name_type_dictionary)
                except Exception as e:
                    if(selected_column.children[0]==None):
                        column_name=selected_column.children[1].children[0].lower()
                    else:
                        column_name=(selected_column.children[0].children[0].lower()
                        +'.'+selected_column.children[1].children[0].lower())
                    raise SelectColumnResolveError(column_name)
                

                column_alias=column_name

                if(selected_column.children[2]!=None):
                    column_alias=selected_column.children[3].children[0]
                projected_columns_dictionary[column_alias]=column_name

        #print output
        
        projected_columns_list=list(projected_columns_dictionary.keys())
        #print header
        header = " | ".join(projected_columns_list)
        line = "+" + "-"*(len(header)+2) + "+"
        print(line)
        print(f"| {header} |")
        print(line)
        #print records
        for record in filtered_record_list:
            values = [str(record.get(projected_columns_dictionary[col], "NULL")) for col in projected_columns_list]
            print(f"| {' | '.join(values)} |")

        print(line)

     


    def show_tables_query(self, items):
        # Print header
        print('-'*65)

        # Iterate over the tables in metaDataDB and print each table's name
        cursor = metaDataDB.cursor()
     
        record = cursor.first()
        while record:
            table_name = record[0].decode('utf-8')
            if(table_name!="-1"):
                print(table_name)
            record = cursor.next()


    
        cursor.close()

        # Print footer
        print('-'*65)
    
    def update_query(self, items):
        print("DB_2020-14094> 'UPDATE' requested")
    def exit_query(self, items):
        sys.exit() #exit run.py during evaluation of first exit_query

    def invalid_query(self, items):
        print("DB_2020-14094> Syntax error")

    #helper function to append table name to column name in record / column_info dictionary
    def append_table_name_to_column_name(self, table_name, column_name_key_dictionary):
        return {table_name+'.'+column_name:value for column_name,value in column_name_key_dictionary.items()}
  

    #helper functions for record validation by where clause
    #any comparison with null value by operator that is not null_operation will make whole clause false
    def validate_record_by_where_clause(self, record, where_clause, column_type_dictionary):
        boolean_expr_result=self.validate_record_by_boolean_expr(record, where_clause.children[1], column_type_dictionary)
        if boolean_expr_result==True:
            return True
        return False


    def validate_record_by_boolean_expr(self, record, boolean_expr, column_type_dictionary):
        for boolean_term in boolean_expr.children:
            if isinstance(boolean_term, Tree):
                boolean_term_result=self.validate_record_by_boolean_term(record, boolean_term, column_type_dictionary)
                if boolean_term_result==None:
                    return None
                if boolean_term_result:
                    return True
        return False
    def validate_record_by_boolean_term(self, record, boolean_term, column_type_dictionary):
        for boolean_factor in boolean_term.children:
            if isinstance(boolean_factor, Tree):
                boolean_factor_result=self.validate_record_by_boolean_factor(record, boolean_factor, column_type_dictionary)
                if boolean_factor_result==None:
                    return None
                if not boolean_factor_result: 
                    return False
        return True

    def validate_record_by_boolean_factor(self, record, boolean_factor, column_type_dictionary):
        boolean_test = boolean_factor.children[1]
        boolean_test_result=self.validate_record_by_boolean_test(record, boolean_test, column_type_dictionary)
        if boolean_test_result==None:
            return None
        if boolean_factor.children[0]==None:
            return boolean_test_result
        return not boolean_test_result
    
    def validate_record_by_boolean_test(self, record, boolean_test, column_type_dictionary):
        if(boolean_test.children[0].data=="predicate"):
            return self.validate_record_by_predicate(record, boolean_test.children[0], column_type_dictionary)
        return self.validate_record_by_boolean_expr(record, boolean_test.children[0].children[1], column_type_dictionary)
    
    def validate_record_by_predicate(self, record, predicate, column_type_dictionary):
        if(predicate.children[0].data=="comparison_predicate"):
            return self.validate_record_by_comparison_predicate(record, predicate.children[0], column_type_dictionary)
        return self.validate_record_by_null_predicate(record, predicate.children[0], column_type_dictionary)

  
    def validate_record_by_comparison_predicate(self, record, comparison_predicate, column_type_dictionary):
        left_operand=comparison_predicate.children[0]
        right_operand=comparison_predicate.children[2]
        operator=comparison_predicate.children[1]

        left_operand_value_type_tuple=self.value_type_tuple_from_comp_operand(record, left_operand, column_type_dictionary)
        right_operand_value_type_tuple=self.value_type_tuple_from_comp_operand(record, right_operand, column_type_dictionary)

        operand_type=left_operand_value_type_tuple[1]
        if(operand_type!=right_operand_value_type_tuple[1]):
            raise WhereIncomparableError()
        left_operand_value=left_operand_value_type_tuple[0]
        right_operand_value=right_operand_value_type_tuple[0]

        #Null values result in Null
        if(left_operand_value==-1 or right_operand_value==-1):
            return None
        
        if(operand_type=="str"):
            if(operator=="="):
                return left_operand_value==right_operand_value
            elif(operator=="!="):
                return left_operand_value!=right_operand_value
            else:
                raise WhereIncomparableError()
        else: 
            if(operand_type=="int"):
                left_operand_value=int(left_operand_value)
                right_operand_value=int(right_operand_value)
            
            #date and int are compared in the same way
            if(operator=="="):
                return left_operand_value==right_operand_value
            elif(operator=="!="):
                return left_operand_value!=right_operand_value
            elif(operator==">"):
                return left_operand_value>right_operand_value
            elif(operator=="<"):
                return left_operand_value<right_operand_value
            elif(operator==">="):
                return left_operand_value>=right_operand_value
            elif(operator=="<="):
                return left_operand_value<=right_operand_value
            else:
                raise WhereIncomparableError()

    def validate_record_by_null_predicate(self, record, null_predicate, column_type_dictionary):
        operand_value_type_tuple=self.value_type_tuple_from_record(record, null_predicate, column_type_dictionary)
        operand_value=operand_value_type_tuple[0]
        validation_result=operand_value==-1 #check null
        if(null_predicate.children[2].children[1]!=None):
            validation_result=not validation_result
        return validation_result




    #helper functions for extracting operand value and type
    def value_type_tuple_from_comp_operand(self, record, operand, column_type_dictionary):
        if(operand.children[0]!=None and operand.children[0].data=="comparable_value"):
            operand_value=operand.children[0].children[0]
            operand_type=operand_value.type.lower()
            if(operand_type=="str"):
                operand_value=operand_value[1:-1]
            return (operand_value, operand_type)
        else:
            return self.value_type_tuple_from_record(record, operand, column_type_dictionary)
        
    
    def value_type_tuple_from_record(self, record, operand, column_type_dictionary):
        column_name=self.extract_column_name_given_table_column(operand, column_type_dictionary)
        operand_value=record.get(column_name,-1)
        operand_type=column_type_dictionary[column_name]
        if(operand_type.startswith("char")):
            operand_type="str"
        return (operand_value, operand_type)
    
    def extract_column_name_given_table_column(self, table_column_tree, column_name_type_dictionary):
        if table_column_tree.children[0]==None:
            column_name=table_column_tree.children[1].children[0].lower()
            column_candidates=[column for column in column_name_type_dictionary if column.endswith('.'+column_name)]
            if(len(column_candidates)>1):
                raise WhereAmbiguousReference()
            elif(len(column_candidates)==0):
                raise WhereColumnNotExistError()
            else:
                column_name=column_candidates[0]
        else:
            table_name_prefix=table_column_tree.children[0].children[0].lower()+'.'
            if(not [column_of_table for column_of_table in column_name_type_dictionary if column_of_table.startswith(table_name_prefix)]):
                raise WhereTableNotSpecifiedError()
            
            column_name=table_name_prefix+table_column_tree.children[1].children[0].lower()
            if(column_name not in column_name_type_dictionary):
                raise WhereColumnNotExistError()
        return column_name
            
    def explain_describe_desc_query(self, table_name):
        
        # Retrieve metadata for the table.
        
        metadata_raw = metaDataDB.get(table_name.encode('utf-8'))
        if not metadata_raw:
            raise NoSuchTable()
        
            
        
        metadata = json.loads(metadata_raw.decode('utf-8'))
        

        # Print the header.
        print('-'*65)
        print(f"table_name [{table_name}]")
        print("{:<15} {:<10} {:<5} {:<10}".format("column_name", "type", "null", "key"))

        # Print columns with associated constraints.
        for column in metadata["columns"]:
            column_name = column["name"]
            column_type = column["type"]
            
            is_null = 'Y'
            key = ''
            if column_name in metadata.get("primary_constraints", []):
                key = 'PRI'
            if any(column_name in constraint["refing_col_list"]  for constraint in metadata.get("foreign_constraints", [])):
                key = key + "/FOR" if key else 'FOR'
            
            if column_name in metadata.get("not_null_constraints", []):
                is_null = 'N'

            print("{:<15} {:<10} {:<5} {:<10}".format(column_name, column_type, is_null, key))

        # Print footer.
        print('-'*65)
    
    
    
current_query="" #stores current query
while(1):#take input from user as long as EXIT query is not given
    if current_query!="":
        input_line=input("")
    else: input_line=input("DB_2020-14094> ") #receive query from user

    current_query+=" "+input_line #append query to current_query
    queries= [query.strip() for query in current_query.split(';')] #split multiple queries into list of queries
    if queries[-1]:
        continue
    else:
        current_query="" #reset current_query
        for query in queries:
                try:
                    output = sql_parser.parse(query)
                    MyTransformer().transform(output)
                except UnexpectedInput as e:
                    print("DB_2020-14094> Syntax error")
                    break
                except Exception as e:
                    print(e.orig_exc.message)
                    break







