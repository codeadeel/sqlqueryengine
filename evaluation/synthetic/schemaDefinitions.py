#!/usr/bin/env python3

# %%
# Inline DDL for the three evaluation databases.
# Kept separate so seed_data.py and any future migration logic
# can import the schemas without pulling in Faker or other heavy deps.

# %%
# E-commerce platform — customers, products, orders, reviews, payments
ECOMMERCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    city VARCHAR(50),
    country VARCHAR(50),
    signup_date DATE NOT NULL,
    is_premium BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    parent_category_id INTEGER REFERENCES categories(category_id)
);
CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category_id INTEGER REFERENCES categories(category_id),
    price NUMERIC(10,2) NOT NULL,
    stock_quantity INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    shipping_address TEXT,
    total_amount NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL
);
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    customer_id INTEGER REFERENCES customers(customer_id),
    rating INTEGER CHECK (rating BETWEEN 1 AND 5),
    review_text TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    payment_method VARCHAR(30) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    payment_date TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'completed'
);
"""

# %%
# University system — departments, faculty, students, courses, enrollments
UNIVERSITY_SCHEMA = """
CREATE TABLE IF NOT EXISTS departments (
    dept_id SERIAL PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    building VARCHAR(50),
    budget NUMERIC(12,2)
);
CREATE TABLE IF NOT EXISTS faculty (
    faculty_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    dept_id INTEGER REFERENCES departments(dept_id),
    title VARCHAR(30),
    hire_date DATE,
    salary NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS students (
    student_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE,
    major_dept_id INTEGER REFERENCES departments(dept_id),
    enrollment_year INTEGER NOT NULL,
    gpa NUMERIC(3,2)
);
CREATE TABLE IF NOT EXISTS courses (
    course_id SERIAL PRIMARY KEY,
    course_code VARCHAR(10) NOT NULL,
    title VARCHAR(100) NOT NULL,
    dept_id INTEGER REFERENCES departments(dept_id),
    credits INTEGER NOT NULL,
    max_enrollment INTEGER DEFAULT 40
);
CREATE TABLE IF NOT EXISTS sections (
    section_id SERIAL PRIMARY KEY,
    course_id INTEGER REFERENCES courses(course_id),
    faculty_id INTEGER REFERENCES faculty(faculty_id),
    semester VARCHAR(10) NOT NULL,
    year INTEGER NOT NULL,
    room VARCHAR(20),
    schedule VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS enrollments (
    enrollment_id SERIAL PRIMARY KEY,
    student_id INTEGER REFERENCES students(student_id),
    section_id INTEGER REFERENCES sections(section_id),
    grade VARCHAR(2),
    enrollment_date DATE DEFAULT CURRENT_DATE
);
CREATE TABLE IF NOT EXISTS prerequisites (
    prereq_id SERIAL PRIMARY KEY,
    course_id INTEGER REFERENCES courses(course_id),
    required_course_id INTEGER REFERENCES courses(course_id)
);
"""

# %%
# Hospital management — departments, doctors, patients, appointments, prescriptions, labs
HOSPITAL_SCHEMA = """
CREATE TABLE IF NOT EXISTS departments (
    dept_id SERIAL PRIMARY KEY,
    name VARCHAR(80) NOT NULL,
    floor INTEGER,
    phone VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS doctors (
    doctor_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    specialization VARCHAR(60),
    dept_id INTEGER REFERENCES departments(dept_id),
    license_number VARCHAR(20) UNIQUE,
    hire_date DATE
);
CREATE TABLE IF NOT EXISTS patients (
    patient_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender VARCHAR(10),
    phone VARCHAR(20),
    address TEXT,
    insurance_provider VARCHAR(60)
);
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    doctor_id INTEGER REFERENCES doctors(doctor_id),
    appointment_date TIMESTAMP NOT NULL,
    status VARCHAR(20) DEFAULT 'scheduled',
    reason TEXT,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS diagnoses (
    diagnosis_id SERIAL PRIMARY KEY,
    appointment_id INTEGER REFERENCES appointments(appointment_id),
    icd_code VARCHAR(10) NOT NULL,
    description TEXT NOT NULL,
    severity VARCHAR(20)
);
CREATE TABLE IF NOT EXISTS medications (
    medication_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    unit_cost NUMERIC(8,2)
);
CREATE TABLE IF NOT EXISTS prescriptions (
    prescription_id SERIAL PRIMARY KEY,
    appointment_id INTEGER REFERENCES appointments(appointment_id),
    medication_id INTEGER REFERENCES medications(medication_id),
    dosage VARCHAR(50),
    duration_days INTEGER,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS lab_results (
    result_id SERIAL PRIMARY KEY,
    patient_id INTEGER REFERENCES patients(patient_id),
    test_name VARCHAR(100) NOT NULL,
    result_value VARCHAR(50),
    unit VARCHAR(20),
    reference_range VARCHAR(50),
    test_date TIMESTAMP DEFAULT NOW(),
    is_abnormal BOOLEAN DEFAULT FALSE
);
"""

# %%
# Schema registry — maps database name to its DDL
SCHEMAS = {
    "eval_ecommerce": ECOMMERCE_SCHEMA,
    "eval_university": UNIVERSITY_SCHEMA,
    "eval_hospital": HOSPITAL_SCHEMA,
}
