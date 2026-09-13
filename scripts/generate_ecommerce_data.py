#!/usr/bin/env python3
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
RNG = random.Random(SEED)

ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = ROOT / "sql"
DATA_DIR = ROOT / "hadoop" / "data"
RESULTS_DIR = ROOT / "results"

# ---------------------------------------------------------------------------
# 1. DIMENSION DATA
# ---------------------------------------------------------------------------

# ---- DimCustomer: 20 customers --------------------------------------------
# (customer_id, full_name, gender, email, city, state_province, country, region, signup_date)
CUSTOMERS = [
    (
        "C001",
        "Amara Okafor",
        "F",
        "amara.okafor@example.com",
        "Lagos",
        "Lagos",
        "Nigeria",
        "Africa",
        date(2024, 3, 14),
    ),
    (
        "C002",
        "Liam O'Connor",
        "M",
        "liam.oconnor@example.com",
        "Dublin",
        "Leinster",
        "Ireland",
        "Europe",
        date(2024, 5, 2),
    ),
    (
        "C003",
        "Priya Sharma",
        "F",
        "priya.sharma@example.com",
        "Mumbai",
        "Maharashtra",
        "India",
        "Asia",
        date(2023, 11, 21),
    ),
    (
        "C004",
        "Diego Fernandez",
        "M",
        "diego.fernandez@example.com",
        "Madrid",
        "Community Madrid",
        "Spain",
        "Europe",
        date(2024, 7, 19),
    ),
    (
        "C005",
        "Sofia Rossi",
        "F",
        "sofia.rossi@example.com",
        "Milan",
        "Lombardy",
        "Italy",
        "Europe",
        date(2024, 1, 8),
    ),
    (
        "C006",
        "Jacob Mensah",
        "M",
        "jacob.mensah@example.com",
        "Accra",
        "Greater Accra",
        "Ghana",
        "Africa",
        date(2024, 9, 30),
    ),
    (
        "C007",
        "Emma Thompson",
        "F",
        "emma.thompson@example.com",
        "New York",
        "New York",
        "USA",
        "North America",
        date(2023, 6, 12),
    ),
    (
        "C008",
        "Noah Williams",
        "M",
        "noah.williams@example.com",
        "London",
        "England",
        "United Kingdom",
        "Europe",
        date(2023, 8, 27),
    ),
    (
        "C009",
        "Aisha Khan",
        "F",
        "aisha.khan@example.com",
        "Karachi",
        "Sindh",
        "Pakistan",
        "Asia",
        date(2024, 2, 16),
    ),
    (
        "C010",
        "Oliver Schmidt",
        "M",
        "oliver.schmidt@example.com",
        "Berlin",
        "Berlin",
        "Germany",
        "Europe",
        date(2024, 4, 5),
    ),
    (
        "C011",
        "Yuki Tanaka",
        "F",
        "yuki.tanaka@example.com",
        "Tokyo",
        "Kanto",
        "Japan",
        "Asia",
        date(2024, 6, 24),
    ),
    (
        "C012",
        "Mateo Silva",
        "M",
        "mateo.silva@example.com",
        "Sao Paulo",
        "Sao Paulo",
        "Brazil",
        "South America",
        date(2024, 8, 11),
    ),
    (
        "C013",
        "Chloe Martin",
        "F",
        "chloe.martin@example.com",
        "Paris",
        "Ile-de-France",
        "France",
        "Europe",
        date(2023, 12, 3),
    ),
    (
        "C014",
        "Ethan Brown",
        "M",
        "ethan.brown@example.com",
        "Toronto",
        "Ontario",
        "Canada",
        "North America",
        date(2024, 10, 22),
    ),
    (
        "C015",
        "Zainab Ahmed",
        "F",
        "zainab.ahmed@example.com",
        "Cairo",
        "Cairo",
        "Egypt",
        "Africa",
        date(2024, 1, 29),
    ),
    (
        "C016",
        "Hannah Chen",
        "F",
        "hannah.chen@example.com",
        "Singapore",
        "Central",
        "Singapore",
        "Asia",
        date(2024, 3, 8),
    ),
    (
        "C017",
        "Lucas Moreau",
        "M",
        "lucas.moreau@example.com",
        "Lyon",
        "Auvergne-Rhone-Alpes",
        "France",
        "Europe",
        date(2024, 5, 17),
    ),
    (
        "C018",
        "Isabella Nguyen",
        "F",
        "isabella.nguyen@example.com",
        "Sydney",
        "New South Wales",
        "Australia",
        "Oceania",
        date(2023, 9, 9),
    ),
    (
        "C019",
        "Omar Haddad",
        "M",
        "omar.haddad@example.com",
        "Dubai",
        "Dubai",
        "UAE",
        "Asia",
        date(2024, 7, 2),
    ),
    (
        "C020",
        "Grace Kim",
        "F",
        "grace.kim@example.com",
        "Seoul",
        "Seoul",
        "South Korea",
        "Asia",
        date(2024, 11, 4),
    ),
]

# ---- DimProduct: 20 products ----------------------------------------------
# (product_id, product_name, category, subcategory, brand, unit_price_usd)
PRODUCTS = [
    ("P001", "Aurora Wireless Headphones", "Electronics", "Audio", "Voltix", 89.99),
    ("P002", 'Nimbus 14" Laptop', "Electronics", "Computing", "Voltix", 649.00),
    ("P003", "Pulse Smartwatch", "Electronics", "Wearables", "Voltix", 159.50),
    ("P004", "Pixel Pro Camera 4K", "Electronics", "Cameras", "Voltix", 429.00),
    ("P005", "UrbanFit Running Jacket", "Clothing", "Outerwear", "KnitWear", 79.99),
    ("P006", "TrailFlex Hiking Boots", "Clothing", "Footwear", "KnitWear", 119.00),
    ("P007", "CoreStretch Yoga Leggings", "Clothing", "Activewear", "KnitWear", 49.99),
    ("P008", "Classic Cotton T-Shirt", "Clothing", "Basics", "KnitWear", 24.99),
    ("P009", "AeroFlow Running Watch", "Sports", "Fitness", "ProSport", 199.99),
    ("P010", "Summit 45L Backpack", "Sports", "Outdoor", "ProSport", 89.50),
    ("P011", "Velocity Cycling Helmet", "Sports", "Cycling", "ProSport", 64.99),
    ("P012", "FitNation Dumbbell Set 20kg", "Sports", "Strength", "ProSport", 139.00),
    (
        "P013",
        "CuisinePro Non-Stick Pan Set",
        "Home & Kitchen",
        "Cookware",
        "HomeEase",
        95.00,
    ),
    (
        "P014",
        "BrewMaster Electric Kettle",
        "Home & Kitchen",
        "Appliances",
        "HomeEase",
        59.99,
    ),
    (
        "P015",
        "ComfortLux Memory Foam Pillow",
        "Home & Kitchen",
        "Bedding",
        "HomeEase",
        44.50,
    ),
    (
        "P016",
        "Organix Bamboo Storage Boxes",
        "Home & Kitchen",
        "Storage",
        "HomeEase",
        34.99,
    ),
    ("P017", "Quantum USB-C Hub 7-in-1", "Electronics", "Accessories", "Voltix", 69.99),
    ("P018", "Prestige Thriller Collection", "Books", "Fiction", "PageTurn", 39.99),
    ("P019", "Data Science Handbook", "Books", "Non-Fiction", "PageTurn", 59.00),
    ("P020", "World Atlas Deluxe Edition", "Books", "Reference", "PageTurn", 79.99),
]

# Popularity weight per product id -- used to weight web VIEW events.
# P009 is deliberately *popular to browse* but bought rarely (conversion insight).
VISIBILITY = {
    p: w
    for p, w in [
        ("P001", 14),
        ("P002", 18),
        ("P003", 15),
        ("P004", 10),
        ("P005", 9),
        ("P006", 8),
        ("P007", 11),
        ("P008", 13),
        ("P009", 22),
        ("P010", 10),
        ("P011", 7),
        ("P012", 9),
        ("P013", 12),
        ("P014", 11),
        ("P015", 9),
        ("P016", 8),
        ("P017", 12),
        ("P018", 7),
        ("P019", 6),
        ("P020", 5),
    ]
}

# ---- DimStore: 10 stores / sales channels ---------------------------------
# (store_id, store_name, channel, city, state_province, country, region, open_date)
STORES = [
    (
        "S001",
        "Voltix Online Store",
        "Online",
        "Global",
        "Global",
        "Global",
        "Global",
        date(2020, 1, 1),
    ),
    (
        "S002",
        "Voltix Mobile App",
        "Mobile App",
        "Global",
        "Global",
        "Global",
        "Global",
        date(2021, 3, 15),
    ),
    (
        "S003",
        "Voltix Marketplace UK",
        "Marketplace",
        "London",
        "England",
        "United Kingdom",
        "Europe",
        date(2021, 6, 1),
    ),
    (
        "S004",
        "Voltix Flagship NYC",
        "Retail Store",
        "New York",
        "New York",
        "USA",
        "North America",
        date(2019, 9, 1),
    ),
    (
        "S005",
        "Voltix Flagship Berlin",
        "Retail Store",
        "Berlin",
        "Berlin",
        "Germany",
        "Europe",
        date(2020, 5, 1),
    ),
    (
        "S006",
        "Voltix Flagship Sydney",
        "Retail Store",
        "Sydney",
        "New South Wales",
        "Australia",
        "Oceania",
        date(2022, 4, 20),
    ),
    (
        "S007",
        "Voltix Marketplace India",
        "Marketplace",
        "Mumbai",
        "Maharashtra",
        "India",
        "Asia",
        date(2022, 8, 10),
    ),
    (
        "S008",
        "Voltix Flagship Dubai",
        "Retail Store",
        "Dubai",
        "Dubai",
        "UAE",
        "Asia",
        date(2023, 1, 18),
    ),
    (
        "S009",
        "Voltix Flagship Tokyo",
        "Retail Store",
        "Tokyo",
        "Kanto",
        "Japan",
        "Asia",
        date(2023, 11, 5),
    ),
    (
        "S010",
        "Voltix Marketplace SA",
        "Marketplace",
        "Sao Paulo",
        "Sao Paulo",
        "Brazil",
        "South America",
        date(2024, 2, 12),
    ),
]

# ---- DimDate: 91 days  (2025-04-01 .. 2025-06-30) -------------------------
DATES = [date(2025, 4, 1) + timedelta(days=i) for i in range(91)]
HOLIDAYS = {date(2025, 4, 20): "Easter Sunday", date(2025, 5, 5): "Spring Bank Holiday"}


def date_row(d: date) -> tuple:
    is_holiday = d in HOLIDAYS
    return (
        d.year * 10000 + d.month * 100 + d.day,  # date_id e.g. 20250401
        d.isoformat(),  # full_date
        d.strftime("%A"),  # day_name
        d.isoweekday(),  # day_of_week (1=Mon..7=Sun)
        d.day,  # day_of_month
        int(d.strftime("%W")),  # week_of_year
        d.month,  # month
        d.strftime("%B"),  # month_name
        (d.month - 1) // 3 + 1,  # quarter
        d.year,  # year
        d.weekday() >= 5,  # is_weekend
        HOLIDAYS.get(d, ""),  # holiday_name
    )


# ---------------------------------------------------------------------------
# 2. FACT DATA  -- FactSales
# ---------------------------------------------------------------------------


def generate_fact_sales():
    """Return list of fact rows and the count of order lines."""
    facts = []
    # Map product -> how many *distinct orders* may contain it (P009 kept rare).
    MAX_ORDERS_PER_PRODUCT = {p[0]: 7 for p in PRODUCTS}
    MAX_ORDERS_PER_PRODUCT["P009"] = 3  # browsed a lot, sold rarely
    MAX_ORDERS_PER_PRODUCT["P020"] = 3
    MAX_ORDERS_PER_PRODUCT["P002"] = 40  # flagship high-ticket product

    orders_containing = {p[0]: 0 for p in PRODUCTS}
    order_no = 0
    sale_id = 1000

    while order_no < 95:
        order_no += 1
        year = 2025
        order_d = RNG.choice(DATES)
        order_id = f"ORD-{year}-{order_no:04d}"
        customer = RNG.choice(CUSTOMERS)
        store = RNG.choice(STORES)
        nlines = RNG.randint(1, 4)

        placed = set()
        for _ in range(nlines):
            # choose a product whose order-cap is not exhausted, plus momentum
            candidates = [
                p
                for p, c in orders_containing.items()
                if c < MAX_ORDERS_PER_PRODUCT[p] and p not in placed
            ]
            if not candidates:
                break
            product_id = RNG.choice(sorted(candidates))
            placed.add(product_id)
            orders_containing[product_id] += 1

            product = next(p for p in PRODUCTS if p[0] == product_id)
            qty = RNG.randint(1, 5)
            discount = RNG.choice([0.00, 0.00, 0.05, 0.05, 0.10, 0.15, 0.20])
            unit_price = product[5]
            sales_amount = round(qty * unit_price * (1 - discount), 2)

            facts.append(
                dict(
                    sale_id=sale_id,
                    order_id=order_id,
                    order_line_no=len(placed),
                    date_id=order_d.year * 10000 + order_d.month * 100 + order_d.day,
                    customer_id=customer[0],
                    product_id=product_id,
                    store_id=store[0],
                    quantity=qty,
                    unit_price=unit_price,
                    discount_rate=discount,
                    sales_amount=sales_amount,
                )
            )
            sale_id += 1

    return facts


FACTS = generate_fact_sales()

# ---------------------------------------------------------------------------
# 3. WEB ACTIVITY DATASET  -- website_activity.csv
# ---------------------------------------------------------------------------

DEVICES = ["Mobile", "Mobile", "Desktop", "Tablet"]
SEARCH_TERMS = [
    "headphones",
    "laptop",
    "laptop offer",
    "smartwatch",
    "camera",
    "jacket",
    "boots",
    "leggings",
    "t-shirt",
    "running watch",
    "backpack",
    "helmet",
    "dumbbells",
    "cookware",
    "kettle",
    "pillow",
    "storage",
    "usb hub",
    "books",
]

CITY_BY_REGION = {
    "Africa": [("Lagos", "Nigeria"), ("Accra", "Ghana"), ("Cairo", "Egypt")],
    "Asia": [
        ("Mumbai", "India"),
        ("Karachi", "Pakistan"),
        ("Tokyo", "Japan"),
        ("Singapore", "Singapore"),
        ("Dubai", "UAE"),
        ("Seoul", "South Korea"),
    ],
    "Europe": [
        ("London", "United Kingdom"),
        ("Paris", "France"),
        ("Berlin", "Germany"),
        ("Madrid", "Spain"),
        ("Milan", "Italy"),
        ("Dublin", "Ireland"),
        ("Lyon", "France"),
    ],
    "North America": [("New York", "USA"), ("Toronto", "Canada")],
    "South America": [("Sao Paulo", "Brazil")],
    "Oceania": [("Sydney", "Australia")],
}


def weighted_product():
    ids = [p[0] for p in PRODUCTS]
    weights = [VISIBILITY[p[0]] for p in PRODUCTS]
    return RNG.choices(ids, weights=weights, k=1)[0]


def build_website_activity():
    """Return list of web-event dicts. PURCHASE events mirror every FactSales line,
    so |PURCHASE| == number of fact rows and every order_id in the log exists in the DW.
    """
    events = []
    session_counter = 0
    # (customer_id, date_id) -> list of (product_id, order_id)
    order_map = {}
    for f in FACTS:
        order_map.setdefault((f["customer_id"], f["date_id"]), []).append(
            (f["product_id"], f["order_id"])
        )

    for d in DATES:
        date_id = d.year * 10000 + d.month * 100 + d.day
        day_start = datetime(d.year, d.month, d.day, 8, 0, 0)
        nsessions = RNG.randint(6, 12)
        customer_session = {}  # customer_id -> session_id (for purchase linkage)
        for _ in range(nsessions):
            session_counter += 1
            session_id = f"SES-{session_counter:05d}"
            device = RNG.choice(DEVICES)
            region, city_options = RNG.choice(list(CITY_BY_REGION.items()))
            city, country = RNG.choice(city_options)
            location = f"{city}_{country}"  # no commas: Hadoop/Hive/Pig friendly
            now = day_start + timedelta(
                hours=RNG.randint(0, 13),
                minutes=RNG.randint(0, 59),
                seconds=RNG.randint(0, 59),
            )
            user = RNG.choice(CUSTOMERS)[0] if RNG.random() < 0.85 else "GUEST"
            if user != "GUEST":
                customer_session[user] = session_id

            # SEARCH first
            events.append(
                dict(
                    timestamp=now.isoformat(sep=" "),
                    customer_id=user,
                    product_id="",
                    event_type="SEARCH",
                    session_id=session_id,
                    device=device,
                    location=location,
                    order_id="",
                )
            )
            now += timedelta(minutes=RNG.randint(0, 3))

            # then a few VIEWs
            viewed = []
            nviews = RNG.randint(1, 4)
            for _ in range(nviews):
                pid = weighted_product()
                viewed.append(pid)
                events.append(
                    dict(
                        timestamp=now.isoformat(sep=" "),
                        customer_id=user,
                        product_id=pid,
                        event_type="VIEW",
                        session_id=session_id,
                        device=device,
                        location=location,
                        order_id="",
                    )
                )
                now += timedelta(minutes=RNG.randint(1, 6))

            # optional ADD_TO_CART
            if viewed and RNG.random() < 0.55:
                events.append(
                    dict(
                        timestamp=now.isoformat(sep=" "),
                        customer_id=user,
                        product_id=RNG.choice(viewed),
                        event_type="ADD_TO_CART",
                        session_id=session_id,
                        device=device,
                        location=location,
                        order_id="",
                    )
                )
                now += timedelta(minutes=RNG.randint(1, 5))

        # PURCHASE events for every real order line sold that day
        for (cust_id, _did), lines in order_map.items():
            if _did != date_id:
                continue
            session_id = customer_session.get(cust_id)
            if session_id is None:
                session_counter += 1
                session_id = f"SES-CHK-{session_counter:05d}"
            device = RNG.choice(DEVICES)
            _region, city_options = RNG.choice(list(CITY_BY_REGION.items()))
            city, country = RNG.choice(city_options)
            location = f"{city}_{country}"
            for pid, order_id in lines:
                ts = (
                    day_start
                    + timedelta(hours=RNG.randint(14, 20), minutes=RNG.randint(0, 59))
                ).isoformat(sep=" ")
                # a view normally precedes a purchase
                events.append(
                    dict(
                        timestamp=ts,
                        customer_id=cust_id,
                        product_id=pid,
                        event_type="VIEW",
                        session_id=session_id,
                        device=device,
                        location=location,
                        order_id="",
                    )
                )
                events.append(
                    dict(
                        timestamp=ts,
                        customer_id=cust_id,
                        product_id=pid,
                        event_type="PURCHASE",
                        session_id=session_id,
                        device=device,
                        location=location,
                        order_id=order_id,
                    )
                )
    return events


WEB_EVENTS = build_website_activity()

# ---------------------------------------------------------------------------
# 4. WRITE OUTPUT FILES
# ---------------------------------------------------------------------------


def emit_sql():
    SQL_DIR.mkdir(parents=True, exist_ok=True)
    out = []
    out.append("-- Auto-generated by scripts/generate_ecommerce_data.py (seed 42).")
    out.append("BEGIN;")
    out.append("")
    out.append("-- DimDate")
    out.append(
        "INSERT INTO DimDate (date_id, full_date, day_name, day_of_week, day_of_month,"
        " week_of_year, month, month_name, quarter, year, is_weekend, holiday_name) VALUES"
    )
    vals = []
    for dr in (date_row(d) for d in DATES):
        vals.append(
            "(%d, DATE '%s', '%s', %d, %d, %d, %d, '%s', %d, %d, %s, %s)"
            % (
                dr[0],
                dr[1],
                dr[2],
                dr[3],
                dr[4],
                dr[5],
                dr[6],
                dr[7],
                dr[8],
                dr[9],
                "TRUE" if dr[10] else "FALSE",
                "'" + dr[11] + "'" if dr[11] else "NULL",
            )
        )
    out.append(",\n".join(vals) + ";")
    out.append("")

    out.append("-- DimCustomer")
    out.append(
        "INSERT INTO DimCustomer (customer_id, full_name, gender, email, city,"
        " state_province, country, region, signup_date) VALUES"
    )
    vals = [
        (
            "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', DATE '%s')"
            % (
                c[0],
                c[1].replace("'", "''"),
                c[2],
                c[3],
                c[4],
                c[5],
                c[6],
                c[7],
                c[8].isoformat(),
            )
        )
        for c in CUSTOMERS
    ]
    out.append(",\n".join(vals) + ";")
    out.append("")

    out.append("-- DimProduct")
    out.append(
        "INSERT INTO DimProduct (product_id, product_name, category, subcategory,"
        " brand, unit_price_USD) VALUES"
    )
    vals = [
        (
            "('%s', '%s', '%s', '%s', '%s', %.2f)"
            % (p[0], p[1].replace("'", "''"), p[2], p[3], p[4], p[5])
        )
        for p in PRODUCTS
    ]
    out.append(",\n".join(vals) + ";")
    out.append("")

    out.append("-- DimStore")
    out.append(
        "INSERT INTO DimStore (store_id, store_name, channel, city, state_province,"
        " country, region, open_date) VALUES"
    )
    vals = [
        (
            "('%s', '%s', '%s', '%s', '%s', '%s', '%s', DATE '%s')"
            % (s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7].isoformat())
        )
        for s in STORES
    ]
    out.append(",\n".join(vals) + ";")
    out.append("")

    out.append("-- FactSales")
    out.append(
        "INSERT INTO FactSales (sale_id, order_id, order_line_no, date_id, customer_id,"
        " product_id, store_id, quantity, unit_price, discount_rate, sales_amount) VALUES"
    )
    vals = [
        "(%d, '%s', %d, %d, '%s', '%s', '%s', %d, %.2f, %.2f, %.2f)"
        % (
            f["sale_id"],
            f["order_id"],
            f["order_line_no"],
            f["date_id"],
            f["customer_id"],
            f["product_id"],
            f["store_id"],
            f["quantity"],
            f["unit_price"],
            f["discount_rate"],
            f["sales_amount"],
        )
        for f in FACTS
    ]
    out.append(",\n".join(vals) + ";")
    out.append("")
    out.append("COMMIT;")
    out.append("")
    out.append("-- Verify row counts")
    out.append("SELECT 'DimDate' AS tbl, COUNT(*) FROM DimDate UNION ALL")
    out.append("SELECT 'DimCustomer', COUNT(*) FROM DimCustomer UNION ALL")
    out.append("SELECT 'DimProduct', COUNT(*) FROM DimProduct UNION ALL")
    out.append("SELECT 'DimStore', COUNT(*) FROM DimStore UNION ALL")
    out.append("SELECT 'FactSales', COUNT(*) FROM FactSales;")
    out.append("")
    path = SQL_DIR / "seed_warehouse_data.sql"
    path.write_text("\n".join(out))
    print(f"wrote {path} ({len(out)} lines)")


def emit_web_activity():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = DATA_DIR / "website_activity.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "timestamp",
                "customer_id",
                "product_id",
                "event_type",
                "session_id",
                "device",
                "location",
                "order_id",
            ],
        )
        writer.writeheader()
        for e in WEB_EVENTS:
            writer.writerow(e)
    print(f"wrote {path} ({len(WEB_EVENTS)} event rows)")


def emit_web_expected_results():
    """Deterministic 'expected output' files for the Hive/Pig sections.

    These reproduce exactly what Hive GROUP BY queries will return for the
    same dataset, so the README results are real outputs, not guesses.
    """
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    from collections import Counter, defaultdict

    views = Counter(e["product_id"] for e in WEB_EVENTS if e["event_type"] == "VIEW")
    customers = Counter(
        e["customer_id"] for e in WEB_EVENTS if e["customer_id"] != "GUEST"
    )
    types = Counter(e["event_type"] for e in WEB_EVENTS)
    devices = Counter(e["device"] for e in WEB_EVENTS)
    locations = Counter(e["location"] for e in WEB_EVENTS)

    def dump(name, header, rows):
        (RESULTS_DIR / name).write_text(
            "\n".join([header] + ["\t".join("{}".format(c) for c in r) for r in rows])
            + "\n"
        )
        print(f"wrote results/{name} ({len(rows)} rows)")

    dump(
        "hive_view_counts.csv",
        "product_id\tview_count",
        sorted(views.items(), key=lambda kv: -kv[1]),
    )
    dump(
        "hive_active_customers.csv",
        "customer_id\tactivity_count",
        sorted(customers.items(), key=lambda kv: -kv[1])[:10],
    )
    dump(
        "hive_event_distribution.csv",
        "event_type\tcount",
        sorted(types.items(), key=lambda kv: -kv[1]),
    )
    dump(
        "hive_device_breakdown.csv",
        "device\tcount",
        sorted(devices.items(), key=lambda kv: -kv[1]),
    )
    dump(
        "hive_location_activity.csv",
        "location\tcount",
        sorted(locations.items(), key=lambda kv: -kv[1]),
    )
    dump(
        "pig_view_counts.csv",
        "product_id\tview_count",
        sorted(views.items(), key=lambda kv: -kv[1]),
    )

    # per-product views needed for the combined analysis
    with open(RESULTS_DIR / "web_views_for_merge.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["product_id", "web_views"])
        for pid, n in sorted(views.items(), key=lambda kv: kv[0]):
            w.writerow([pid, n])


def _main():
    emit_sql()
    emit_web_activity()
    emit_web_expected_results()
    print(
        f"fact lines: {len(FACTS)} | orders: {len({f['order_id'] for f in FACTS})} | "
        f"days: {len(DATES)} | web events: {len(WEB_EVENTS)}"
    )


if __name__ == "__main__":
    _main()

