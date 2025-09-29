import pathlib
import sqlglot

def test_sqlglot():
    try:
        sql = "SELEC * FROM table;"
        parsed = sqlglot.parse_one(sql)
    except Exception as e:
        print(e)

def test_transpile():
    src_folder = pathlib.Path(__file__).parent.parent.parent / "data" / "spark-project" / "sources" / "raw_active_users.sql"
    with open(src_folder, "r") as f:
        sql = f.read()
        print(sql)
        parsed = sqlglot.transpile(sql, read='spark', write='flink')
        print(parsed)

if __name__ == "__main__":
    test_transpile()