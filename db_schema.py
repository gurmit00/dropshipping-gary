import mysql.connector

DB_CONFIG = {
    "host": "50.87.172.208",
    "port": 3306,
    "user": "obaqucmy_upwork",
    "password": "Letmein123!",
    "database": "obaqucmy_fashion",
}

# List your tables here
TABLES = ["product_details", "products_from_category", "scrape_runs"]
OUTFILE = "schema_only.sql"

def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    with open(OUTFILE, "w", encoding="utf-8") as f:
        for table in TABLES:
            cursor.execute(f"SHOW CREATE TABLE `{table}`")
            result = cursor.fetchone()
            if result:
                f.write("-- ----------------------------\n")
                f.write(f"-- Table structure for `{table}`\n")
                f.write("-- ----------------------------\n")
                f.write(result[1] + ";\n\n")
    cursor.close()
    conn.close()
    print(f"Exported CREATE TABLE statements to {OUTFILE}")

if __name__ == "__main__":
    main()
