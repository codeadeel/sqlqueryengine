#!/usr/bin/env python3

# %%
# Hospital evaluation questions — 40 questions across 4 difficulty tiers
# 10 easy, 12 medium, 10 hard, 8 extra hard

HOSPITAL_QUESTIONS = [
    # Easy (10)
    {
        "difficulty": "easy",
        "question": "How many patients are in the system?",
        "gold_query": "SELECT COUNT(*) FROM patients;"
    },
    {
        "difficulty": "easy",
        "question": "How many doctors work at the hospital?",
        "gold_query": "SELECT COUNT(*) FROM doctors;"
    },
    {
        "difficulty": "easy",
        "question": "List all hospital departments.",
        "gold_query": "SELECT name FROM departments ORDER BY name;"
    },
    {
        "difficulty": "easy",
        "question": "How many appointments have been completed?",
        "gold_query": "SELECT COUNT(*) FROM appointments WHERE status = 'completed';"
    },
    {
        "difficulty": "easy",
        "question": "What is the most expensive medication?",
        "gold_query": "SELECT name, unit_cost FROM medications ORDER BY unit_cost DESC LIMIT 1;"
    },
    {
        "difficulty": "easy",
        "question": "How many patients have insurance from Blue Cross?",
        "gold_query": "SELECT COUNT(*) FROM patients WHERE insurance_provider = 'Blue Cross';"
    },
    {
        "difficulty": "easy",
        "question": "How many abnormal lab results have been recorded?",
        "gold_query": "SELECT COUNT(*) FROM lab_results WHERE is_abnormal = TRUE;"
    },
    {
        "difficulty": "easy",
        "question": "How many male patients are there?",
        "gold_query": "SELECT COUNT(*) FROM patients WHERE gender = 'Male';"
    },
    {
        "difficulty": "easy",
        "question": "What is the total number of prescriptions issued?",
        "gold_query": "SELECT COUNT(*) FROM prescriptions;"
    },
    {
        "difficulty": "easy",
        "question": "How many different types of lab tests have been performed?",
        "gold_query": "SELECT COUNT(DISTINCT test_name) FROM lab_results;"
    },

    # Medium (12)
    {
        "difficulty": "medium",
        "question": "Which doctor has the most appointments?",
        "gold_query": "SELECT d.first_name, d.last_name, COUNT(a.appointment_id) AS appt_count FROM doctors d JOIN appointments a ON d.doctor_id = a.doctor_id GROUP BY d.doctor_id, d.first_name, d.last_name ORDER BY appt_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What is the most common diagnosis?",
        "gold_query": "SELECT description, COUNT(*) AS count FROM diagnoses GROUP BY description ORDER BY count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "How many appointments are there per department?",
        "gold_query": "SELECT dep.name, COUNT(a.appointment_id) AS appt_count FROM departments dep JOIN doctors d ON dep.dept_id = d.dept_id JOIN appointments a ON d.doctor_id = a.doctor_id GROUP BY dep.dept_id, dep.name ORDER BY appt_count DESC;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average number of appointments per patient?",
        "gold_query": "SELECT ROUND(AVG(appt_count), 2) FROM (SELECT patient_id, COUNT(*) AS appt_count FROM appointments GROUP BY patient_id) sub;"
    },
    {
        "difficulty": "medium",
        "question": "Which medication is prescribed most frequently?",
        "gold_query": "SELECT m.name, COUNT(p.prescription_id) AS prescription_count FROM medications m JOIN prescriptions p ON m.medication_id = p.medication_id GROUP BY m.medication_id, m.name ORDER BY prescription_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What is the distribution of appointment statuses?",
        "gold_query": "SELECT status, COUNT(*) AS count, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage FROM appointments GROUP BY status ORDER BY count DESC;"
    },
    {
        "difficulty": "medium",
        "question": "How many patients have had more than 5 appointments?",
        "gold_query": "SELECT COUNT(*) FROM (SELECT patient_id FROM appointments GROUP BY patient_id HAVING COUNT(*) > 5) sub;"
    },
    {
        "difficulty": "medium",
        "question": "What is the most common reason for appointments?",
        "gold_query": "SELECT reason, COUNT(*) AS count FROM appointments GROUP BY reason ORDER BY count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "Which insurance provider covers the most patients?",
        "gold_query": "SELECT insurance_provider, COUNT(*) AS patient_count FROM patients WHERE insurance_provider IS NOT NULL GROUP BY insurance_provider ORDER BY patient_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average prescription duration in days?",
        "gold_query": "SELECT ROUND(AVG(duration_days), 2) FROM prescriptions;"
    },
    {
        "difficulty": "medium",
        "question": "How many diagnoses have a severity of 'severe'?",
        "gold_query": "SELECT COUNT(*) FROM diagnoses WHERE severity = 'severe';"
    },
    {
        "difficulty": "medium",
        "question": "Which department has the most doctors?",
        "gold_query": "SELECT dep.name, COUNT(d.doctor_id) AS doctor_count FROM departments dep JOIN doctors d ON dep.dept_id = d.dept_id GROUP BY dep.dept_id, dep.name ORDER BY doctor_count DESC LIMIT 1;"
    },

    # Hard (10)
    {
        "difficulty": "hard",
        "question": "Which doctors have diagnosed the same patient more than once with different conditions?",
        "gold_query": """
            SELECT d.first_name, d.last_name, p.first_name AS patient_first, p.last_name AS patient_last,
                   COUNT(DISTINCT diag.icd_code) AS different_diagnoses
            FROM doctors d
            JOIN appointments a ON d.doctor_id = a.doctor_id
            JOIN diagnoses diag ON a.appointment_id = diag.appointment_id
            JOIN patients p ON a.patient_id = p.patient_id
            GROUP BY d.doctor_id, d.first_name, d.last_name, p.patient_id, p.first_name, p.last_name
            HAVING COUNT(DISTINCT diag.icd_code) > 1
            ORDER BY different_diagnoses DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the average time between appointments for patients who have had more than 3 visits?",
        "gold_query": """
            WITH patient_appts AS (
                SELECT patient_id, appointment_date,
                       LEAD(appointment_date) OVER (PARTITION BY patient_id ORDER BY appointment_date) AS next_appt
                FROM appointments
            ),
            frequent_patients AS (
                SELECT patient_id FROM appointments GROUP BY patient_id HAVING COUNT(*) > 3
            )
            SELECT ROUND(AVG(EXTRACT(EPOCH FROM (next_appt - appointment_date)) / 86400)::NUMERIC, 2) AS avg_days_between
            FROM patient_appts pa
            JOIN frequent_patients fp ON pa.patient_id = fp.patient_id
            WHERE pa.next_appt IS NOT NULL;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find the top 5 most costly patients based on total medication costs from their prescriptions.",
        "gold_query": """
            SELECT p.first_name, p.last_name,
                   ROUND(SUM(m.unit_cost * pr.duration_days), 2) AS total_med_cost
            FROM patients p
            JOIN appointments a ON p.patient_id = a.patient_id
            JOIN prescriptions pr ON a.appointment_id = pr.appointment_id
            JOIN medications m ON pr.medication_id = m.medication_id
            GROUP BY p.patient_id, p.first_name, p.last_name
            ORDER BY total_med_cost DESC
            LIMIT 5;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the month-over-month trend of appointments for the last 12 months?",
        "gold_query": """
            WITH monthly AS (
                SELECT DATE_TRUNC('month', appointment_date)::date AS month,
                       COUNT(*) AS appt_count
                FROM appointments
                WHERE appointment_date >= '2025-06-01'::timestamp - INTERVAL '12 months'
                GROUP BY DATE_TRUNC('month', appointment_date)
            )
            SELECT month, appt_count,
                   appt_count - LAG(appt_count) OVER (ORDER BY month) AS change_from_prev
            FROM monthly
            ORDER BY month;
        """
    },
    {
        "difficulty": "hard",
        "question": "For each doctor, what is their most commonly diagnosed condition?",
        "gold_query": """
            WITH doc_diag AS (
                SELECT d.doctor_id, d.first_name, d.last_name,
                       diag.description,
                       COUNT(*) AS diag_count,
                       ROW_NUMBER() OVER (PARTITION BY d.doctor_id ORDER BY COUNT(*) DESC) AS rn
                FROM doctors d
                JOIN appointments a ON d.doctor_id = a.doctor_id
                JOIN diagnoses diag ON a.appointment_id = diag.appointment_id
                GROUP BY d.doctor_id, d.first_name, d.last_name, diag.description
            )
            SELECT first_name, last_name, description AS top_diagnosis, diag_count
            FROM doc_diag
            WHERE rn = 1
            ORDER BY last_name;
        """
    },
    {
        "difficulty": "hard",
        "question": "Which patients have had both abnormal and normal lab results for the same test type?",
        "gold_query": """
            SELECT p.first_name, p.last_name, lr.test_name
            FROM patients p
            JOIN lab_results lr ON p.patient_id = lr.patient_id
            GROUP BY p.patient_id, p.first_name, p.last_name, lr.test_name
            HAVING COUNT(DISTINCT lr.is_abnormal) = 2
            ORDER BY p.last_name, lr.test_name;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the no-show rate for each day of the week?",
        "gold_query": """
            SELECT TO_CHAR(appointment_date, 'Day') AS day_of_week,
                   EXTRACT(DOW FROM appointment_date)::INTEGER AS day_num,
                   COUNT(*) AS total_appts,
                   COUNT(*) FILTER (WHERE status = 'no_show') AS no_shows,
                   ROUND(COUNT(*) FILTER (WHERE status = 'no_show') * 100.0 / COUNT(*), 2) AS no_show_rate
            FROM appointments
            GROUP BY TO_CHAR(appointment_date, 'Day'), EXTRACT(DOW FROM appointment_date)
            ORDER BY day_num;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find medications that are commonly prescribed together in the same appointment (co-prescribed).",
        "gold_query": """
            SELECT m1.name AS med_1, m2.name AS med_2, COUNT(*) AS co_prescription_count
            FROM prescriptions p1
            JOIN prescriptions p2 ON p1.appointment_id = p2.appointment_id AND p1.medication_id < p2.medication_id
            JOIN medications m1 ON p1.medication_id = m1.medication_id
            JOIN medications m2 ON p2.medication_id = m2.medication_id
            GROUP BY m1.medication_id, m1.name, m2.medication_id, m2.name
            HAVING COUNT(*) >= 3
            ORDER BY co_prescription_count DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the average age of patients by department?",
        "gold_query": """
            SELECT dep.name,
                   ROUND(AVG(EXTRACT(YEAR FROM AGE(p.date_of_birth)))::NUMERIC, 1) AS avg_age
            FROM departments dep
            JOIN doctors d ON dep.dept_id = d.dept_id
            JOIN appointments a ON d.doctor_id = a.doctor_id
            JOIN patients p ON a.patient_id = p.patient_id
            GROUP BY dep.dept_id, dep.name
            ORDER BY avg_age DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "Rank patients by their number of distinct diagnoses.",
        "gold_query": """
            SELECT p.first_name, p.last_name,
                   COUNT(DISTINCT diag.icd_code) AS distinct_diagnoses,
                   RANK() OVER (ORDER BY COUNT(DISTINCT diag.icd_code) DESC) AS patient_rank
            FROM patients p
            JOIN appointments a ON p.patient_id = a.patient_id
            JOIN diagnoses diag ON a.appointment_id = diag.appointment_id
            GROUP BY p.patient_id, p.first_name, p.last_name
            ORDER BY patient_rank
            LIMIT 20;
        """
    },

    # Extra Hard (8)
    {
        "difficulty": "extra_hard",
        "question": "Identify patients who had an abnormal lab result within 30 days after a prescription was issued for a medication in the same category.",
        "gold_query": """
            SELECT p.first_name, p.last_name, m.name AS medication, m.category, lr.test_name, lr.test_date::date
            FROM patients p
            JOIN appointments a ON p.patient_id = a.patient_id
            JOIN prescriptions pr ON a.appointment_id = pr.appointment_id
            JOIN medications m ON pr.medication_id = m.medication_id
            JOIN lab_results lr ON p.patient_id = lr.patient_id
            WHERE lr.is_abnormal = TRUE
              AND lr.test_date BETWEEN a.appointment_date AND a.appointment_date + INTERVAL '30 days'
            GROUP BY p.patient_id, p.first_name, p.last_name, m.name, m.category, lr.test_name, lr.test_date
            ORDER BY p.last_name, lr.test_date;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Calculate the readmission rate: percentage of patients who had another appointment within 30 days of a completed appointment.",
        "gold_query": """
            WITH completed AS (
                SELECT patient_id, appointment_date,
                       LEAD(appointment_date) OVER (PARTITION BY patient_id ORDER BY appointment_date) AS next_appt
                FROM appointments
                WHERE status = 'completed'
            )
            SELECT ROUND(
                COUNT(DISTINCT CASE WHEN EXTRACT(EPOCH FROM (next_appt - appointment_date)) / 86400 <= 30 THEN patient_id END) * 100.0 /
                NULLIF(COUNT(DISTINCT patient_id), 0), 2
            ) AS readmission_rate_pct
            FROM completed
            WHERE next_appt IS NOT NULL;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "For each medication category, what is the average treatment duration, total prescriptions, and the percentage of prescriptions that exceed 30 days?",
        "gold_query": """
            SELECT m.category,
                   COUNT(pr.prescription_id) AS total_prescriptions,
                   ROUND(AVG(pr.duration_days), 2) AS avg_duration,
                   ROUND(COUNT(*) FILTER (WHERE pr.duration_days > 30) * 100.0 / COUNT(*), 2) AS pct_over_30_days
            FROM medications m
            JOIN prescriptions pr ON m.medication_id = pr.medication_id
            GROUP BY m.category
            ORDER BY total_prescriptions DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Find doctors whose patients have a higher than average rate of abnormal lab results.",
        "gold_query": """
            WITH overall_abnormal_rate AS (
                SELECT COUNT(*) FILTER (WHERE is_abnormal) * 1.0 / COUNT(*) AS rate
                FROM lab_results
            ),
            doc_abnormal_rate AS (
                SELECT d.doctor_id, d.first_name, d.last_name,
                       COUNT(lr.result_id) AS total_labs,
                       COUNT(lr.result_id) FILTER (WHERE lr.is_abnormal) AS abnormal_labs,
                       COUNT(lr.result_id) FILTER (WHERE lr.is_abnormal) * 1.0 / NULLIF(COUNT(lr.result_id), 0) AS abnormal_rate
                FROM doctors d
                JOIN appointments a ON d.doctor_id = a.doctor_id
                JOIN patients p ON a.patient_id = p.patient_id
                JOIN lab_results lr ON p.patient_id = lr.patient_id
                GROUP BY d.doctor_id, d.first_name, d.last_name
                HAVING COUNT(lr.result_id) >= 10
            )
            SELECT dar.first_name, dar.last_name, dar.total_labs, dar.abnormal_labs,
                   ROUND(dar.abnormal_rate * 100, 2) AS abnormal_rate_pct,
                   ROUND(oar.rate * 100, 2) AS hospital_avg_pct
            FROM doc_abnormal_rate dar
            CROSS JOIN overall_abnormal_rate oar
            WHERE dar.abnormal_rate > oar.rate
            ORDER BY dar.abnormal_rate DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Analyze the seasonal pattern of diagnoses: which diagnoses are significantly more common in certain quarters of the year?",
        "gold_query": """
            WITH quarterly_diag AS (
                SELECT diag.description,
                       EXTRACT(QUARTER FROM a.appointment_date)::INTEGER AS quarter,
                       COUNT(*) AS count
                FROM diagnoses diag
                JOIN appointments a ON diag.appointment_id = a.appointment_id
                GROUP BY diag.description, EXTRACT(QUARTER FROM a.appointment_date)
            ),
            diag_avg AS (
                SELECT description, AVG(count) AS avg_count, STDDEV(count) AS std_count
                FROM quarterly_diag
                GROUP BY description
                HAVING COUNT(DISTINCT quarter) >= 3
            )
            SELECT qd.description, qd.quarter, qd.count,
                   ROUND(da.avg_count, 2) AS avg_count,
                   ROUND((qd.count - da.avg_count) / NULLIF(da.std_count, 0), 2) AS z_score
            FROM quarterly_diag qd
            JOIN diag_avg da ON qd.description = da.description
            WHERE ABS((qd.count - da.avg_count) / NULLIF(da.std_count, 0)) > 1
            ORDER BY ABS((qd.count - da.avg_count) / NULLIF(da.std_count, 0)) DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Build a patient risk score based on: number of diagnoses, number of abnormal lab results, and number of distinct medications prescribed. Show the top 20 highest risk patients.",
        "gold_query": """
            WITH patient_diagnoses AS (
                SELECT a.patient_id, COUNT(DISTINCT diag.diagnosis_id) AS diag_count
                FROM appointments a
                JOIN diagnoses diag ON a.appointment_id = diag.appointment_id
                GROUP BY a.patient_id
            ),
            patient_abnormal AS (
                SELECT patient_id, COUNT(*) AS abnormal_count
                FROM lab_results
                WHERE is_abnormal = TRUE
                GROUP BY patient_id
            ),
            patient_meds AS (
                SELECT a.patient_id, COUNT(DISTINCT pr.medication_id) AS med_count
                FROM appointments a
                JOIN prescriptions pr ON a.appointment_id = pr.appointment_id
                GROUP BY a.patient_id
            )
            SELECT p.first_name, p.last_name,
                   COALESCE(pd.diag_count, 0) AS diagnoses,
                   COALESCE(pa.abnormal_count, 0) AS abnormal_labs,
                   COALESCE(pm.med_count, 0) AS distinct_medications,
                   COALESCE(pd.diag_count, 0) + COALESCE(pa.abnormal_count, 0) * 2 + COALESCE(pm.med_count, 0) AS risk_score
            FROM patients p
            LEFT JOIN patient_diagnoses pd ON p.patient_id = pd.patient_id
            LEFT JOIN patient_abnormal pa ON p.patient_id = pa.patient_id
            LEFT JOIN patient_meds pm ON p.patient_id = pm.patient_id
            ORDER BY risk_score DESC, p.patient_id
            LIMIT 20;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Identify potential drug interactions: patients who were prescribed medications from more than 3 different categories within a 30-day window.",
        "gold_query": """
            WITH prescription_details AS (
                SELECT a.patient_id, a.appointment_date, m.category, m.name AS med_name
                FROM appointments a
                JOIN prescriptions pr ON a.appointment_id = pr.appointment_id
                JOIN medications m ON pr.medication_id = m.medication_id
            )
            SELECT DISTINCT p.first_name, p.last_name,
                   pd1.appointment_date::date AS window_start,
                   COUNT(DISTINCT pd2.category) AS categories_in_window
            FROM prescription_details pd1
            JOIN prescription_details pd2 ON pd1.patient_id = pd2.patient_id
                AND pd2.appointment_date BETWEEN pd1.appointment_date AND pd1.appointment_date + INTERVAL '30 days'
            JOIN patients p ON pd1.patient_id = p.patient_id
            GROUP BY p.patient_id, p.first_name, p.last_name, pd1.appointment_date
            HAVING COUNT(DISTINCT pd2.category) > 3
            ORDER BY categories_in_window DESC, p.last_name;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Compare doctor workload fairness: calculate the Gini coefficient of appointment distribution across doctors within each department.",
        "gold_query": """
            WITH doc_appts AS (
                SELECT d.dept_id, d.doctor_id, COUNT(a.appointment_id) AS appt_count
                FROM doctors d
                LEFT JOIN appointments a ON d.doctor_id = a.doctor_id
                GROUP BY d.dept_id, d.doctor_id
            ),
            dept_stats AS (
                SELECT dept_id,
                       ARRAY_AGG(appt_count ORDER BY appt_count) AS counts,
                       COUNT(*) AS n,
                       SUM(appt_count) AS total
                FROM doc_appts
                GROUP BY dept_id
                HAVING COUNT(*) >= 2
            )
            SELECT dep.name,
                   ds.n AS doctors,
                   ds.total AS total_appointments,
                   ROUND(
                       (2.0 * SUM(
                           (gs.ordinality) * (ds.counts[gs.ordinality])
                       ) / (ds.n * ds.total) - (ds.n + 1.0) / ds.n)::NUMERIC, 4
                   ) AS gini_coefficient
            FROM dept_stats ds
            JOIN departments dep ON ds.dept_id = dep.dept_id
            CROSS JOIN LATERAL generate_series(1, ds.n) WITH ORDINALITY AS gs(val, ordinality)
            GROUP BY dep.dept_id, dep.name, ds.n, ds.total
            ORDER BY gini_coefficient DESC;
        """
    },
]
