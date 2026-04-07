#!/usr/bin/env python3

# %%
# Importing Necessary Libraries
import json
import logging
import os
import random
from datetime import timedelta

from faker import Faker

from evalConfig import adminConnect, dbConnect, RESULTS_BASE_DIR, EVAL_DATABASES
from schemaDefinitions import SCHEMAS

logger = logging.getLogger(__name__)

fake = Faker()
random.seed(42)
Faker.seed(42)


# %%
# Database and schema bootstrap
def createDatabases():
    """Create the three evaluation databases if they do not already exist."""
    logger.info("Creating evaluation databases")
    conn = adminConnect()
    cur = conn.cursor()
    for db in EVAL_DATABASES:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {db}")
            logger.info("  Created %s", db)
        else:
            logger.info("  %s already exists", db)
    conn.close()


def applySchemas():
    """Apply DDL schemas and truncate all tables for a clean seed."""
    logger.info("Applying schemas")
    for db, ddl in SCHEMAS.items():
        conn = dbConnect(db)
        cur = conn.cursor()
        cur.execute(ddl)
        # Truncate all tables so re-runs don't hit unique constraint violations
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        )
        tables = [r[0] for r in cur.fetchall()]
        if tables:
            cur.execute(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE")
        conn.close()
        logger.info("  %s schema applied (tables truncated)", db)


# %%
# Ecommerce seeder
def seedEcommerce():
    """
    Populate ``eval_ecommerce`` with synthetic data.

    ~100 customers, 20 categories, 200 products, 500 orders,
    ~1500 order items, 300 reviews, 500 payments.
    """
    logger.info("Seeding eval_ecommerce")
    conn = dbConnect("eval_ecommerce")
    cur = conn.cursor()

    # Categories
    topCategories = [
        "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
        "Toys", "Food & Beverages", "Health", "Automotive", "Music",
    ]
    for name in topCategories:
        cur.execute("INSERT INTO categories (name) VALUES (%s)", (name,))
    subCategories = [
        ("Smartphones", 1), ("Laptops", 1), ("Men's Wear", 2), ("Women's Wear", 2),
        ("Fiction", 3), ("Non-Fiction", 3), ("Furniture", 4), ("Kitchen", 4),
        ("Running", 5), ("Team Sports", 5),
    ]
    for name, parentID in subCategories:
        cur.execute("INSERT INTO categories (name, parent_category_id) VALUES (%s, %s)", (name, parentID))
    totalCategories = len(topCategories) + len(subCategories)

    # Customers
    countries = ["USA", "Canada", "UK", "Germany", "France", "Australia", "Japan", "Brazil", "India", "Mexico"]
    customerIDs = []
    usedEmails = set()
    for i in range(100):
        first, last = fake.first_name(), fake.last_name()
        email = f"{first.lower()}.{last.lower()}.{i}@{fake.free_email_domain()}"
        while email in usedEmails:
            email = f"{first.lower()}{random.randint(1, 999)}@{fake.free_email_domain()}"
        usedEmails.add(email)
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email, city, country, signup_date, is_premium) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING customer_id",
            (first, last, email, fake.city(), random.choice(countries),
             fake.date_between(start_date="-2y", end_date="today"), random.random() < 0.2),
        )
        customerIDs.append(cur.fetchone()[0])

    # Products
    productNames = [
        "Wireless Mouse", "Bluetooth Speaker", "USB-C Hub", "Mechanical Keyboard", "Monitor Stand",
        "Webcam HD", "External SSD", "Phone Case", "Screen Protector", "Power Bank",
        "Running Shoes", "Yoga Mat", "Water Bottle", "Backpack", "Sunglasses",
        "T-Shirt", "Hoodie", "Jeans", "Sneakers", "Watch",
        "Novel", "Cookbook", "Textbook", "Journal", "Planner",
        "Desk Lamp", "Throw Pillow", "Candle Set", "Wall Art", "Rug",
        "Basketball", "Tennis Racket", "Dumbbells", "Jump Rope", "Helmet",
        "Action Figure", "Board Game", "Puzzle", "Lego Set", "Stuffed Animal",
    ]
    products = []
    for i in range(200):
        price = round(random.uniform(5.0, 500.0), 2)
        cur.execute(
            "INSERT INTO products (name, category_id, price, stock_quantity, created_at, is_active) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING product_id",
            (random.choice(productNames) + f" V{i+1}", random.randint(1, totalCategories),
             price, random.randint(0, 200), fake.date_time_between(start_date="-2y", end_date="now"),
             random.random() < 0.9),
        )
        products.append((cur.fetchone()[0], price))

    # Orders + order items + payments
    statuses = ["pending", "shipped", "delivered", "cancelled", "returned"]
    paymentMethods = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
    for _ in range(500):
        orderDate = fake.date_time_between(start_date="-2y", end_date="now")
        status = random.choices(statuses, weights=[10, 20, 50, 10, 10])[0]
        cur.execute(
            "INSERT INTO orders (customer_id, order_date, status, shipping_address, total_amount) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING order_id",
            (random.choice(customerIDs), orderDate, status, fake.address().replace("\n", ", "), 0),
        )
        orderID = cur.fetchone()[0]

        total = 0.0
        for prodID, prodPrice in random.sample(products, min(random.randint(1, 6), len(products))):
            qty = random.randint(1, 5)
            total += qty * float(prodPrice)
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)",
                (orderID, prodID, qty, prodPrice),
            )
        total = round(total, 2)
        cur.execute("UPDATE orders SET total_amount = %s WHERE order_id = %s", (total, orderID))

        payStatus = "completed" if status in ("shipped", "delivered") else random.choice(["completed", "pending", "failed"])
        cur.execute(
            "INSERT INTO payments (order_id, payment_method, amount, payment_date, status) VALUES (%s,%s,%s,%s,%s)",
            (orderID, random.choice(paymentMethods), total,
             orderDate + timedelta(minutes=random.randint(1, 60)), payStatus),
        )

    # Reviews
    for _ in range(300):
        cur.execute(
            "INSERT INTO reviews (product_id, customer_id, rating, review_text, created_at) VALUES (%s,%s,%s,%s,%s)",
            (random.choice(products)[0], random.choice(customerIDs),
             random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 35, 35])[0],
             fake.sentence(nb_words=random.randint(5, 20)),
             fake.date_time_between(start_date="-2y", end_date="now")),
        )

    conn.close()
    logger.info("  eval_ecommerce seeded")


# %%
# University seeder
def seedUniversity():
    """
    Populate ``eval_university`` with synthetic data.

    5 departments, 20 faculty, 200 students, 30 courses,
    50 sections, 800 enrollments, 7 prerequisites.
    """
    logger.info("Seeding eval_university")
    conn = dbConnect("eval_university")
    cur = conn.cursor()

    # Departments
    deptData = [
        ("Computer Science", "Engineering Hall", 1500000),
        ("Mathematics", "Science Building", 800000),
        ("Physics", "Science Building", 950000),
        ("English", "Arts Building", 600000),
        ("Biology", "Life Sciences Center", 1100000),
    ]
    deptIDs = []
    for name, building, budget in deptData:
        cur.execute("INSERT INTO departments (name, building, budget) VALUES (%s,%s,%s) RETURNING dept_id",
                    (name, building, budget))
        deptIDs.append(cur.fetchone()[0])

    # Faculty
    titles = ["Professor", "Associate Professor", "Assistant Professor", "Lecturer"]
    facultyIDs = []
    for i in range(20):
        cur.execute(
            "INSERT INTO faculty (first_name, last_name, dept_id, title, hire_date, salary) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING faculty_id",
            (fake.first_name(), fake.last_name(), deptIDs[i % 5], random.choice(titles),
             fake.date_between(start_date="-15y", end_date="-1y"), round(random.uniform(55000, 160000), 2)),
        )
        facultyIDs.append(cur.fetchone()[0])

    # Students
    studentIDs = []
    usedEmails = set()
    for i in range(200):
        first, last = fake.first_name(), fake.last_name()
        email = f"{first.lower()}.{last.lower()}.{i}@university.edu"
        while email in usedEmails:
            email = f"{first.lower()}{random.randint(1, 9999)}@university.edu"
        usedEmails.add(email)
        cur.execute(
            "INSERT INTO students (first_name, last_name, email, major_dept_id, enrollment_year, gpa) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING student_id",
            (first, last, email, random.choice(deptIDs),
             random.choice([2022, 2023, 2024, 2025]), min(round(random.uniform(1.5, 4.0), 2), 4.0)),
        )
        studentIDs.append(cur.fetchone()[0])

    # Courses
    courseData = [
        ("CS101", "Intro to Programming", 1, 3), ("CS201", "Data Structures", 1, 3),
        ("CS301", "Algorithms", 1, 3), ("CS401", "Machine Learning", 1, 3),
        ("CS350", "Databases", 1, 3), ("CS450", "Operating Systems", 1, 3),
        ("MATH101", "Calculus I", 2, 4), ("MATH201", "Calculus II", 2, 4),
        ("MATH301", "Linear Algebra", 2, 3), ("MATH401", "Real Analysis", 2, 3),
        ("MATH250", "Probability", 2, 3), ("MATH350", "Statistics", 2, 3),
        ("PHYS101", "Physics I", 3, 4), ("PHYS201", "Physics II", 3, 4),
        ("PHYS301", "Quantum Mechanics", 3, 3), ("PHYS350", "Thermodynamics", 3, 3),
        ("PHYS401", "Electrodynamics", 3, 3), ("PHYS250", "Optics", 3, 3),
        ("ENG101", "English Composition", 4, 3), ("ENG201", "American Literature", 4, 3),
        ("ENG301", "Creative Writing", 4, 3), ("ENG250", "World Literature", 4, 3),
        ("ENG350", "Shakespeare", 4, 3), ("ENG401", "Literary Theory", 4, 3),
        ("BIO101", "Intro to Biology", 5, 4), ("BIO201", "Genetics", 5, 3),
        ("BIO301", "Microbiology", 5, 3), ("BIO350", "Ecology", 5, 3),
        ("BIO401", "Biochemistry", 5, 4), ("BIO250", "Cell Biology", 5, 3),
    ]
    courseIDs = []
    for code, title, deptIdx, credits in courseData:
        cur.execute(
            "INSERT INTO courses (course_code, title, dept_id, credits, max_enrollment) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING course_id",
            (code, title, deptIDs[deptIdx - 1], credits, random.randint(25, 60)),
        )
        courseIDs.append(cur.fetchone()[0])

    # Prerequisites
    for cidIdx, reqIdx in [(1, 0), (2, 1), (3, 2), (7, 6), (9, 8), (13, 12), (14, 13)]:
        cur.execute("INSERT INTO prerequisites (course_id, required_course_id) VALUES (%s,%s)",
                    (courseIDs[cidIdx], courseIDs[reqIdx]))

    # Sections
    semesters = [("Fall", 2024), ("Spring", 2025), ("Fall", 2025), ("Spring", 2026)]
    rooms = ["Room 101", "Room 102", "Room 201", "Room 202", "Room 301",
             "Lecture Hall A", "Lecture Hall B", "Lab 1", "Lab 2"]
    schedules = ["MWF 9:00-9:50", "MWF 10:00-10:50", "MWF 11:00-11:50", "TR 9:30-10:45",
                 "TR 11:00-12:15", "TR 1:00-2:15", "MWF 1:00-1:50", "MWF 2:00-2:50"]
    sectionIDs = []
    for _ in range(50):
        sem, year = random.choice(semesters)
        cur.execute(
            "INSERT INTO sections (course_id, faculty_id, semester, year, room, schedule) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING section_id",
            (random.choice(courseIDs), random.choice(facultyIDs), sem, year,
             random.choice(rooms), random.choice(schedules)),
        )
        sectionIDs.append(cur.fetchone()[0])

    # Enrollments
    grades = ["A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D", "F", None]
    gradeWeights = [15, 10, 12, 15, 10, 8, 8, 5, 4, 3, 10]
    enrolledPairs = set()
    count = 0
    while count < 800:
        pair = (random.choice(studentIDs), random.choice(sectionIDs))
        if pair in enrolledPairs:
            continue
        enrolledPairs.add(pair)
        cur.execute(
            "INSERT INTO enrollments (student_id, section_id, grade, enrollment_date) VALUES (%s,%s,%s,%s)",
            (pair[0], pair[1], random.choices(grades, weights=gradeWeights)[0],
             fake.date_between(start_date="-2y", end_date="today")),
        )
        count += 1

    conn.close()
    logger.info("  eval_university seeded")


# %%
# Hospital seeder
def seedHospital():
    """
    Populate ``eval_hospital`` with synthetic data.

    5 departments, 15 doctors, 300 patients, 1000 appointments,
    ~600 diagnoses, 30 medications, 800 prescriptions, 500 lab results.
    """
    logger.info("Seeding eval_hospital")
    conn = dbConnect("eval_hospital")
    cur = conn.cursor()

    # Departments
    hospDepts = [
        ("Emergency Medicine", 1, "555-0101"), ("Cardiology", 2, "555-0102"),
        ("Neurology", 3, "555-0103"), ("Orthopedics", 2, "555-0104"),
        ("Pediatrics", 1, "555-0105"),
    ]
    deptIDs = []
    for name, floor, phone in hospDepts:
        cur.execute("INSERT INTO departments (name, floor, phone) VALUES (%s,%s,%s) RETURNING dept_id",
                    (name, floor, phone))
        deptIDs.append(cur.fetchone()[0])

    # Doctors
    specializations = [
        "Emergency Medicine", "Cardiology", "Neurology", "Orthopedic Surgery",
        "Pediatrics", "Internal Medicine", "General Surgery", "Dermatology",
        "Radiology", "Anesthesiology", "Psychiatry", "Oncology",
        "Pulmonology", "Gastroenterology", "Endocrinology",
    ]
    doctorIDs = []
    for i in range(15):
        cur.execute(
            "INSERT INTO doctors (first_name, last_name, specialization, dept_id, license_number, hire_date) "
            "VALUES (%s,%s,%s,%s,%s,%s) RETURNING doctor_id",
            (fake.first_name(), fake.last_name(), specializations[i], deptIDs[i % 5],
             f"MD-{10000 + i}", fake.date_between(start_date="-20y", end_date="-1y")),
        )
        doctorIDs.append(cur.fetchone()[0])

    # Patients
    insurers = ["Blue Cross", "Aetna", "UnitedHealth", "Cigna", "Humana", "Kaiser", "Medicare", "Medicaid", None]
    patientIDs = []
    for _ in range(300):
        cur.execute(
            "INSERT INTO patients (first_name, last_name, date_of_birth, gender, phone, address, insurance_provider) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING patient_id",
            (fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=1, maximum_age=90),
             random.choices(["Male", "Female", "Other"], weights=[48, 48, 4])[0],
             fake.phone_number()[:20], fake.address().replace("\n", ", "), random.choice(insurers)),
        )
        patientIDs.append(cur.fetchone()[0])

    # Appointments
    reasons = [
        "Routine checkup", "Follow-up visit", "Chest pain", "Headache", "Back pain",
        "Fever", "Skin rash", "Joint pain", "Cough", "Abdominal pain", "Dizziness",
        "Fatigue", "Annual physical", "Vaccination", "Lab results review", "Pre-surgery consultation",
    ]
    for _ in range(1000):
        cur.execute(
            "INSERT INTO appointments (patient_id, doctor_id, appointment_date, status, reason, notes) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (random.choice(patientIDs), random.choice(doctorIDs),
             fake.date_time_between(start_date="-1y", end_date="now"),
             random.choices(["scheduled", "completed", "cancelled", "no_show"], weights=[15, 60, 15, 10])[0],
             random.choice(reasons), fake.sentence() if random.random() < 0.5 else None),
        )

    cur.execute("SELECT appointment_id FROM appointments WHERE status = 'completed'")
    completedAppts = [r[0] for r in cur.fetchall()]

    # Diagnoses
    icdCodes = [
        ("J06.9", "Acute upper respiratory infection", "mild"),
        ("I10", "Essential hypertension", "moderate"),
        ("E11.9", "Type 2 diabetes mellitus", "moderate"),
        ("M54.5", "Low back pain", "mild"),
        ("J18.9", "Pneumonia", "severe"),
        ("K21.0", "Gastro-esophageal reflux disease", "mild"),
        ("F41.1", "Generalized anxiety disorder", "moderate"),
        ("G43.909", "Migraine, unspecified", "moderate"),
        ("J45.20", "Mild intermittent asthma", "mild"),
        ("N39.0", "Urinary tract infection", "mild"),
        ("I25.10", "Coronary artery disease", "severe"),
        ("M79.3", "Panniculitis", "mild"),
        ("E78.5", "Hyperlipidemia", "moderate"),
        ("R10.9", "Unspecified abdominal pain", "mild"),
        ("S52.501A", "Fracture of lower end of radius", "moderate"),
    ]
    for apptID in random.sample(completedAppts, min(600, len(completedAppts))):
        icd, desc, sev = random.choice(icdCodes)
        cur.execute("INSERT INTO diagnoses (appointment_id, icd_code, description, severity) VALUES (%s,%s,%s,%s)",
                    (apptID, icd, desc, sev))

    # Medications
    medData = [
        ("Amoxicillin", "Antibiotic", 5.50), ("Ibuprofen", "NSAID", 3.25),
        ("Metformin", "Antidiabetic", 8.00), ("Lisinopril", "ACE Inhibitor", 6.75),
        ("Atorvastatin", "Statin", 12.50), ("Omeprazole", "Proton Pump Inhibitor", 7.00),
        ("Amlodipine", "Calcium Channel Blocker", 9.25), ("Metoprolol", "Beta Blocker", 8.50),
        ("Albuterol", "Bronchodilator", 15.00), ("Prednisone", "Corticosteroid", 4.50),
        ("Gabapentin", "Anticonvulsant", 11.00), ("Sertraline", "SSRI", 10.25),
        ("Tramadol", "Opioid Analgesic", 14.00), ("Ciprofloxacin", "Antibiotic", 9.75),
        ("Losartan", "ARB", 7.50), ("Furosemide", "Diuretic", 5.00),
        ("Warfarin", "Anticoagulant", 6.25), ("Levothyroxine", "Thyroid Hormone", 8.75),
        ("Clopidogrel", "Antiplatelet", 13.50), ("Pantoprazole", "Proton Pump Inhibitor", 9.00),
        ("Hydrochlorothiazide", "Diuretic", 4.25), ("Simvastatin", "Statin", 11.50),
        ("Montelukast", "Leukotriene Inhibitor", 12.00), ("Duloxetine", "SNRI", 15.50),
        ("Azithromycin", "Antibiotic", 7.25), ("Acetaminophen", "Analgesic", 2.50),
        ("Cephalexin", "Antibiotic", 6.00), ("Doxycycline", "Antibiotic", 5.75),
        ("Fluticasone", "Corticosteroid", 18.00), ("Insulin Glargine", "Insulin", 45.00),
    ]
    medIDs = []
    for name, cat, cost in medData:
        cur.execute("INSERT INTO medications (name, category, unit_cost) VALUES (%s,%s,%s) RETURNING medication_id",
                    (name, cat, cost))
        medIDs.append(cur.fetchone()[0])

    # Prescriptions
    dosages = ["500mg twice daily", "250mg three times daily", "10mg once daily", "20mg once daily",
               "5mg twice daily", "100mg at bedtime", "1 tablet daily", "2 tablets twice daily", "5ml three times daily"]
    for apptID in random.choices(completedAppts, k=800):
        cur.execute(
            "INSERT INTO prescriptions (appointment_id, medication_id, dosage, duration_days, notes) VALUES (%s,%s,%s,%s,%s)",
            (apptID, random.choice(medIDs), random.choice(dosages),
             random.choice([5, 7, 10, 14, 21, 30, 60, 90]),
             fake.sentence() if random.random() < 0.3 else None),
        )

    # Lab results
    labTests = [
        ("Complete Blood Count", "cells/mcL", "4500-11000"), ("Hemoglobin", "g/dL", "12.0-17.5"),
        ("Blood Glucose", "mg/dL", "70-100"), ("HbA1c", "%", "4.0-5.6"),
        ("Total Cholesterol", "mg/dL", "125-200"), ("LDL Cholesterol", "mg/dL", "0-100"),
        ("HDL Cholesterol", "mg/dL", "40-60"), ("Triglycerides", "mg/dL", "0-150"),
        ("Creatinine", "mg/dL", "0.7-1.3"), ("BUN", "mg/dL", "7-20"),
        ("TSH", "mIU/L", "0.4-4.0"), ("ALT", "U/L", "7-56"),
        ("AST", "U/L", "10-40"), ("Sodium", "mEq/L", "136-145"),
        ("Potassium", "mEq/L", "3.5-5.0"),
    ]
    for _ in range(500):
        testName, unit, refRange = random.choice(labTests)
        isAbnormal = random.random() < 0.2
        if isAbnormal:
            resultVal = str(round(random.uniform(0.1, 300.0), 1))
        else:
            try:
                low, high = refRange.split("-")
                resultVal = str(round(random.uniform(float(low), float(high)), 1))
            except ValueError:
                resultVal = str(round(random.uniform(1.0, 100.0), 1))
        cur.execute(
            "INSERT INTO lab_results (patient_id, test_name, result_value, unit, reference_range, test_date, is_abnormal) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (random.choice(patientIDs), testName, resultVal, unit, refRange,
             fake.date_time_between(start_date="-1y", end_date="now"), isAbnormal),
        )

    conn.close()
    logger.info("  eval_hospital seeded")


# %%
# Data manifest — row counts per table, saved to results/
def saveManifest():
    """Query row counts from every table across all eval databases and save to JSON."""
    os.makedirs(RESULTS_BASE_DIR, exist_ok=True)
    manifest = {}
    for dbname in EVAL_DATABASES:
        conn = dbConnect(dbname)
        cur = conn.cursor()
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name")
        counts = {}
        for (table,) in cur.fetchall():
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        manifest[dbname] = counts
        conn.close()

    path = os.path.join(RESULTS_BASE_DIR, "data_manifest.json")
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)
    logger.info("Manifest saved to %s", path)
    for db, tables in manifest.items():
        logger.info("  %s: %d rows across %d tables", db, sum(tables.values()), len(tables))


# %%
# Execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    createDatabases()
    applySchemas()
    seedEcommerce()
    seedUniversity()
    seedHospital()
    saveManifest()
    logger.info("Seeding complete")
