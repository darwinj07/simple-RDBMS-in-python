# simplified-RDBMS-in-python
Implementation of simplified RDBMS in Python, using Lark for parsing and Oracle Berkeley DB.
Supports following statements:

```CREATE TABLE, DROP TABLE, EXPLAIN/DESCRIBE/DESC, SHOW TABLES, INSERT TABLE, SELECT```

| Message Type                           | Message                                                          |
|----------------------------------------|------------------------------------------------------------------|
| SyntaxError                            | Syntax error                                                     |
| CreateTableSuccess(#tableName)         | '#tableName' table is created                                    |
| DuplicateColumnDefError                | Create table has failed: column definition is duplicated         |
| DuplicatePrimaryKeyDefError            | Create table has failed: primary key definition is duplicated     |
| ReferenceTypeError                     | Create table has failed: foreign key references wrong type        |
| ReferenceNonPrimaryKeyError            | Create table has failed: foreign key references non primary key column |
| ReferenceColumnExistenceError          | Create table has failed: foreign key references non existing column |
| ReferenceTableExistenceError           | Create table has failed: foreign key references non existing table |
| NonExistingColumnDefError(#colName)    | Create table has failed: '#colName' does not exist in column definition |
| TableExistenceError                    | Create table has failed: table with the same name already exists |
| CharLengthError                        | Char length should be over 0                                     |
| DropSuccess(#tableName)                | '#tableName' table is dropped                                    |
| NoSuchTable                            | No such table                                                    |
| DropReferencedTableError(#tableName)   | Drop table has failed: '#tableName' is referenced by other table |
| SelectTableExistenceError(#tableName)  | Selection has failed: '#tableName' does not exist                 |
| InsertResult                           | 1 row inserted                                                   |
| InsertTypeMismatchError                | Insertion has failed: Types are not matched                       |
| InsertColumnExistenceError(#colName)  | Insertion has failed: '#colName' does not exist                  |
| InsertColumnNonNullableError(#colName) | Insertion has failed: '#colName' is not nullable                 |
| DeleteResult(#count)                   | ‘#count’ row(s) deleted                                          |
| SelectTableExistenceError(#tableName) | Selection has failed: '#tableName' does not exist                |
| SelectColumnResolveError(#colName)    | Selection has failed: fail to resolve '#colName'                 |
| WhereIncomparableError                 | Where clause trying to compare incomparable values               |
| WhereTableNotSpecified                 | Where clause trying to reference tables which are not specified  |
| WhereColumnNotExist                    | Where clause trying to reference non existing column              |
| WhereAmbiguousReference                | Where clause contains ambiguous reference                         |
| InsertDuplicatePrimaryKeyError            | Insertion has failed: Primary key duplication         |
| InsertReferentialIntegrityError           | Insertion has failed: Referential integrity violation |
| DeleteReferentialIntegrityPassed(#count)  | ‘#count’ row(s) are not deleted due to referential integrity |

## Installing Dependencies
### macOS
```bash
brew install berkeley-db@18
YES_I_HAVE_THE_RIGHT_TO_USE_THIS_BERKELEY_DB_VERSION=1 pip install -r requirements.txt
```

### Linux(Debian)
```bash
sudo apt-get install libdb-dev
pip install -r requirements.txt
```

## Running
```bash
python3 run.py
```
