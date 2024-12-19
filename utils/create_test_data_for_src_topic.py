import random
import string
from datetime import datetime

def generate_random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def generate_random_email():
    """Generate a random email address."""
    name = generate_random_string(8)
    domain = random.choice(['example.com', 'test.com', 'sample.org'])
    return f"{name}@{domain}"

def generate_random_phone_number():
    """Generate a random phone number."""
    return ''.join(random.choices(string.digits, k=10))

def generate_random_boolean():
    """Generate a random boolean value."""
    return random.choice([True, False])

def generate_random_bigint():
    """Generate a random bigint (8 bytes integer)."""
    return random.randint(0, 2**63 - 1)

def generate_random_timestamp():
    """Generate a random timestamp in milliseconds since epoch."""
    start = datetime(2020, 1, 1)
    end = datetime.now()
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return int((start + timedelta(seconds=random_seconds)).timestamp() * 1000)

def generate_random_date():
    """Generate a random date in year, month, day format."""
    year = random.randint(2020, 2023)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Simplified to avoid month/day validation
    return year, month, day

def generate_random_data():
    """Generate a single row of test data."""
    user_id = generate_random_string()
    user_name = f"User{generate_random_string(3)}"
    pin = generate_random_string(6)
    last_nm = generate_random_string(10)
    first_nm = generate_random_string(10)
    depart_num = generate_random_string(5)
    employee_num = generate_random_string(7)
    mailbox = generate_random_email()
    approve_pin = generate_random_string(8)
    login_lockedout = generate_random_boolean()
    appr_lockedout = generate_random_boolean()
    req_login_chg = generate_random_boolean()
    req_appr_chg = generate_random_boolean()
    supervisor_id = generate_random_string()
    account_disabled = generate_random_boolean()
    title_nm = f"Title{generate_random_string(3)}"
    date_created = generate_random_bigint()
    created_by = f"Creator{generate_random_string(3)}"
    date_deleted = generate_random_bigint() if random.choice([True, False]) else None
    deleted_by = f"Deleter{generate_random_string(3)}" if date_deleted else None
    active = random.randint(0, 1)
    locked_by = f"Locker{generate_random_string(3)}" if random.choice([True, False]) else None
    locked_date = generate_random_bigint() if locked_by else None
    temp_password = generate_random_string(12)
    external_user = generate_random_boolean()
    timezone = random.choice(['UTC', 'PST', 'EST'])
    title = f"Title{generate_random_string(3)}"
    suffix = f"Suffix{generate_random_string(2)}"
    address1 = f"{random.randint(1, 999)} {generate_random_string()} St."
    address2 = generate_random_string() if random.choice([True, False]) else None
    city = generate_random_string()
    state = generate_random_string(3).upper()
    zipcode = ''.join(random.choices(string.digits, k=5))
    country = f"Country{generate_random_string(3)}"
    phone = generate_random_phone_number()
    fax = generate_random_phone_number() if random.choice([True, False]) else None
    company = f"Company{generate_random_string(4)}"
    is_third_party = generate_random_boolean()
    op = random.choice(['INSERT', 'UPDATE', 'DELETE'])
    db = "tdc_sys_user_raw"
    snapshot = generate_random_bigint()
    source_ts_ms = generate_random_timestamp()
    ts_ms = generate_random_timestamp()
    deleted = generate_random_string() if random.choice([True, False]) else None
    keys = f"Keys{generate_random_string(5)}"
    pin_chg = generate_random_bigint()
    approval_pin_chg = generate_random_bigint()
    date_chged = generate_random_bigint()
    time_chged = generate_random_timestamp()
    login_tries = random.randint(0, 10)
    appr_tries = random.randint(0, 5)
    last_logout = generate_random_bigint()
    year, month, day = generate_random_date()
    rescued_data = generate_random_string() if random.choice([True, False]) else None
    dl_landed_at = generate_random_timestamp()

    return (user_id, user_name, pin, last_nm, first_nm, depart_num, employee_num,
            mailbox, approve_pin, login_lockedout, appr_lockedout, req_login_chg,
            req_appr_chg, supervisor_id, account_disabled, title_nm, date_created,
            created_by, date_deleted, deleted_by, active, locked_by, locked_date,
            temp_password, external_user, timezone, title, suffix, address1, address2,
            city, state, zipcode, country, phone, fax, company, is_third_party,
            op, db, snapshot, source_ts_ms, ts_ms, deleted, keys, pin_chg, approval_pin_chg,
            date_chged, time_chged, login_tries, appr_tries, last_logout, year, month, day,
            rescued_data, dl_landed_at)

def generate_sql_insert_statement(data):
    """Generate an SQL INSERT statement from a tuple of data."""
    placeholders = ', '.join(['%s'] * len(data))
    values = ', '.join([f"'{item}'" if isinstance(item, str) else str(item) for item in data])
    return f"INSERT INTO tdc_sys_user_raw ({', '.join(columns)}) VALUES ({values});"

# Define the columns based on the table schema
columns = [
    "user_id", "user_name", "pin", "last_nm", "first_nm", "depart_num", "employee_num",
    "mailbox", "approve_pin", "login_lockedout", "appr_lockedout", "req_login_chg",
    "req_appr_chg", "supervisor_id", "account_disabled", "title_nm", "date_created",
    "created_by", "date_deleted", "deleted_by", "active", "locked_by", "locked_date",
    "temp_password", "external_user", "timezone", "title", "suffix", "address1", "address2",
    "city", "state", "zipcode", "country", "phone", "fax", "company", "is_third_party",
    "__op", "__db", "__snapshot", "__source_ts_ms", "__ts_ms", "__deleted", "__keys", "pin_chg",
    "approval_pin_chg", "date_chged", "time_chged", "login_tries", "appr_tries", "last_logout",
    "year", "month", "day", "_rescued_data", "dl_landed_at"
]

# Generate a number of test data rows
num_rows = 10
test_data = [generate_random_data() for _ in range(num_rows)]

# Generate SQL insert statements and write to a file
with open('insert_statements.sql', 'w') as f:
    for row in test_data:
        sql_statement = generate_sql_insert_statement(row)
        f.write(sql_statement + '\n')

print(f"Generated {num_rows} SQL insert statements in 'insert_statements.sql'.")