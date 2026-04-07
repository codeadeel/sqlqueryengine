#!/usr/bin/env python3

# %%
# University evaluation questions — 40 questions across 4 difficulty tiers
# 10 easy, 12 medium, 10 hard, 8 extra hard

UNIVERSITY_QUESTIONS = [
    # Easy (10)
    {
        "difficulty": "easy",
        "question": "How many students are enrolled in the university?",
        "gold_query": "SELECT COUNT(*) FROM students;"
    },
    {
        "difficulty": "easy",
        "question": "List all department names.",
        "gold_query": "SELECT name FROM departments ORDER BY name;"
    },
    {
        "difficulty": "easy",
        "question": "What is the average GPA of all students?",
        "gold_query": "SELECT ROUND(AVG(gpa), 2) FROM students;"
    },
    {
        "difficulty": "easy",
        "question": "How many courses are offered?",
        "gold_query": "SELECT COUNT(*) FROM courses;"
    },
    {
        "difficulty": "easy",
        "question": "What is the highest faculty salary?",
        "gold_query": "SELECT MAX(salary) FROM faculty;"
    },
    {
        "difficulty": "easy",
        "question": "How many sections are scheduled for Spring 2025?",
        "gold_query": "SELECT COUNT(*) FROM sections WHERE semester = 'Spring' AND year = 2025;"
    },
    {
        "difficulty": "easy",
        "question": "How many students enrolled in 2024?",
        "gold_query": "SELECT COUNT(*) FROM students WHERE enrollment_year = 2024;"
    },
    {
        "difficulty": "easy",
        "question": "What is the total budget across all departments?",
        "gold_query": "SELECT SUM(budget) FROM departments;"
    },
    {
        "difficulty": "easy",
        "question": "How many faculty members have the title Professor?",
        "gold_query": "SELECT COUNT(*) FROM faculty WHERE title = 'Professor';"
    },
    {
        "difficulty": "easy",
        "question": "How many courses have 4 credits?",
        "gold_query": "SELECT COUNT(*) FROM courses WHERE credits = 4;"
    },

    # Medium (12)
    {
        "difficulty": "medium",
        "question": "How many students are majoring in each department?",
        "gold_query": "SELECT d.name, COUNT(s.student_id) AS student_count FROM departments d LEFT JOIN students s ON d.dept_id = s.major_dept_id GROUP BY d.dept_id, d.name ORDER BY student_count DESC;"
    },
    {
        "difficulty": "medium",
        "question": "Which faculty member teaches the most sections?",
        "gold_query": "SELECT f.first_name, f.last_name, COUNT(s.section_id) AS section_count FROM faculty f JOIN sections s ON f.faculty_id = s.faculty_id GROUP BY f.faculty_id, f.first_name, f.last_name ORDER BY section_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average GPA per department?",
        "gold_query": "SELECT d.name, ROUND(AVG(s.gpa), 2) AS avg_gpa FROM departments d JOIN students s ON d.dept_id = s.major_dept_id GROUP BY d.dept_id, d.name ORDER BY avg_gpa DESC;"
    },
    {
        "difficulty": "medium",
        "question": "How many students have received an A grade in any course?",
        "gold_query": "SELECT COUNT(DISTINCT e.student_id) FROM enrollments e WHERE e.grade = 'A';"
    },
    {
        "difficulty": "medium",
        "question": "Which courses have the most enrolled students?",
        "gold_query": "SELECT c.course_code, c.title, COUNT(e.enrollment_id) AS enrollment_count FROM courses c JOIN sections s ON c.course_id = s.course_id JOIN enrollments e ON s.section_id = e.section_id GROUP BY c.course_id, c.course_code, c.title ORDER BY enrollment_count DESC LIMIT 5;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average salary per faculty title?",
        "gold_query": "SELECT title, ROUND(AVG(salary), 2) AS avg_salary FROM faculty GROUP BY title ORDER BY avg_salary DESC;"
    },
    {
        "difficulty": "medium",
        "question": "How many students have a GPA above 3.5?",
        "gold_query": "SELECT COUNT(*) FROM students WHERE gpa > 3.5;"
    },
    {
        "difficulty": "medium",
        "question": "Which department has the highest total faculty salary expense?",
        "gold_query": "SELECT d.name, ROUND(SUM(f.salary), 2) AS total_salary FROM departments d JOIN faculty f ON d.dept_id = f.dept_id GROUP BY d.dept_id, d.name ORDER BY total_salary DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "List courses that have prerequisites.",
        "gold_query": "SELECT DISTINCT c.course_code, c.title FROM courses c JOIN prerequisites p ON c.course_id = p.course_id ORDER BY c.course_code;"
    },
    {
        "difficulty": "medium",
        "question": "What is the grade distribution across all enrollments?",
        "gold_query": "SELECT grade, COUNT(*) AS count FROM enrollments WHERE grade IS NOT NULL GROUP BY grade ORDER BY count DESC;"
    },
    {
        "difficulty": "medium",
        "question": "How many sections are taught in each building?",
        "gold_query": "SELECT SPLIT_PART(s.room, ' ', 1) || ' ' || SPLIT_PART(s.room, ' ', 2) AS building_area, COUNT(*) AS section_count FROM sections s GROUP BY building_area ORDER BY section_count DESC;"
    },
    {
        "difficulty": "medium",
        "question": "Which students have enrolled in more than 5 course sections?",
        "gold_query": "SELECT s.first_name, s.last_name, COUNT(e.enrollment_id) AS enrollment_count FROM students s JOIN enrollments e ON s.student_id = e.student_id GROUP BY s.student_id, s.first_name, s.last_name HAVING COUNT(e.enrollment_id) > 5 ORDER BY enrollment_count DESC;"
    },

    # Hard (10)
    {
        "difficulty": "hard",
        "question": "Rank departments by average student GPA, including only departments with at least 10 students.",
        "gold_query": """
            SELECT d.name, ROUND(AVG(s.gpa), 2) AS avg_gpa,
                   RANK() OVER (ORDER BY AVG(s.gpa) DESC) AS gpa_rank
            FROM departments d
            JOIN students s ON d.dept_id = s.major_dept_id
            GROUP BY d.dept_id, d.name
            HAVING COUNT(s.student_id) >= 10
            ORDER BY gpa_rank;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find students who have taken courses in at least 3 different departments.",
        "gold_query": """
            SELECT s.first_name, s.last_name, COUNT(DISTINCT c.dept_id) AS dept_count
            FROM students s
            JOIN enrollments e ON s.student_id = e.student_id
            JOIN sections sec ON e.section_id = sec.section_id
            JOIN courses c ON sec.course_id = c.course_id
            GROUP BY s.student_id, s.first_name, s.last_name
            HAVING COUNT(DISTINCT c.dept_id) >= 3
            ORDER BY dept_count DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the fail rate (grade F) for each course, and which courses have a fail rate above 5%?",
        "gold_query": """
            WITH course_grades AS (
                SELECT c.course_code, c.title,
                       COUNT(*) AS total_enrolled,
                       COUNT(*) FILTER (WHERE e.grade = 'F') AS fail_count
                FROM courses c
                JOIN sections s ON c.course_id = s.course_id
                JOIN enrollments e ON s.section_id = e.section_id
                WHERE e.grade IS NOT NULL
                GROUP BY c.course_id, c.course_code, c.title
            )
            SELECT course_code, title, total_enrolled, fail_count,
                   ROUND(fail_count * 100.0 / total_enrolled, 2) AS fail_rate
            FROM course_grades
            WHERE fail_count * 100.0 / total_enrolled > 5
            ORDER BY fail_rate DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "Which faculty members have taught courses in a department different from their own?",
        "gold_query": """
            SELECT DISTINCT f.first_name, f.last_name, d1.name AS own_dept, d2.name AS teaching_dept
            FROM faculty f
            JOIN departments d1 ON f.dept_id = d1.dept_id
            JOIN sections s ON f.faculty_id = s.faculty_id
            JOIN courses c ON s.course_id = c.course_id
            JOIN departments d2 ON c.dept_id = d2.dept_id
            WHERE f.dept_id != c.dept_id
            ORDER BY f.last_name;
        """
    },
    {
        "difficulty": "hard",
        "question": "For each semester-year combination, what is the average enrollment count per section?",
        "gold_query": """
            SELECT s.semester, s.year,
                   ROUND(AVG(enrollment_count), 2) AS avg_enrollment
            FROM sections s
            JOIN (
                SELECT section_id, COUNT(*) AS enrollment_count
                FROM enrollments
                GROUP BY section_id
            ) ec ON s.section_id = ec.section_id
            GROUP BY s.semester, s.year
            ORDER BY s.year, s.semester;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find the GPA percentile rank for each student within their major department.",
        "gold_query": """
            SELECT s.first_name, s.last_name, d.name AS department, s.gpa,
                   ROUND((PERCENT_RANK() OVER (PARTITION BY s.major_dept_id ORDER BY s.gpa) * 100)::NUMERIC, 2) AS percentile_rank
            FROM students s
            JOIN departments d ON s.major_dept_id = d.dept_id
            ORDER BY d.name, percentile_rank DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "Which courses have a prerequisite chain of depth 2 or more (A requires B which requires C)?",
        "gold_query": """
            SELECT c1.course_code AS course, c2.course_code AS requires, c3.course_code AS which_requires
            FROM prerequisites p1
            JOIN prerequisites p2 ON p1.required_course_id = p2.course_id
            JOIN courses c1 ON p1.course_id = c1.course_id
            JOIN courses c2 ON p1.required_course_id = c2.course_id
            JOIN courses c3 ON p2.required_course_id = c3.course_id
            ORDER BY c1.course_code;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the correlation between number of credits and average grade (treating A=4, B=3, C=2, D=1, F=0)?",
        "gold_query": """
            WITH grade_points AS (
                SELECT c.credits,
                       CASE e.grade
                           WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
                           WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
                           WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
                           WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
                       END AS points
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
                JOIN courses c ON s.course_id = c.course_id
                WHERE e.grade IS NOT NULL
            )
            SELECT credits, ROUND(AVG(points), 2) AS avg_grade_points, COUNT(*) AS enrollments
            FROM grade_points
            WHERE points IS NOT NULL
            GROUP BY credits
            ORDER BY credits;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find students whose GPA is higher than the average GPA of their major department.",
        "gold_query": """
            SELECT s.first_name, s.last_name, d.name AS department, s.gpa,
                   ROUND(dept_avg.avg_gpa, 2) AS dept_avg_gpa
            FROM students s
            JOIN departments d ON s.major_dept_id = d.dept_id
            JOIN (
                SELECT major_dept_id, AVG(gpa) AS avg_gpa
                FROM students
                GROUP BY major_dept_id
            ) dept_avg ON s.major_dept_id = dept_avg.major_dept_id
            WHERE s.gpa > dept_avg.avg_gpa
            ORDER BY d.name, s.gpa DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "What are the most common course pairings that students take in the same semester?",
        "gold_query": """
            WITH student_semester_courses AS (
                SELECT e.student_id, s.semester, s.year, s.course_id
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
            )
            SELECT c1.course_code AS course_1, c2.course_code AS course_2, COUNT(*) AS pair_count
            FROM student_semester_courses ssc1
            JOIN student_semester_courses ssc2 ON ssc1.student_id = ssc2.student_id
                AND ssc1.semester = ssc2.semester AND ssc1.year = ssc2.year
                AND ssc1.course_id < ssc2.course_id
            JOIN courses c1 ON ssc1.course_id = c1.course_id
            JOIN courses c2 ON ssc2.course_id = c2.course_id
            GROUP BY c1.course_code, c2.course_code
            HAVING COUNT(*) >= 3
            ORDER BY pair_count DESC
            LIMIT 10;
        """
    },

    # Extra Hard (8)
    {
        "difficulty": "extra_hard",
        "question": "For each course, show the grade distribution as percentages, but only for courses where the fail rate (F grade) exceeds 5%.",
        "gold_query": """
            WITH course_grades AS (
                SELECT c.course_code, c.title, e.grade, COUNT(*) AS cnt,
                       SUM(COUNT(*)) OVER (PARTITION BY c.course_id) AS total
                FROM courses c
                JOIN sections s ON c.course_id = s.course_id
                JOIN enrollments e ON s.section_id = e.section_id
                WHERE e.grade IS NOT NULL
                GROUP BY c.course_id, c.course_code, c.title, e.grade
            ),
            fail_courses AS (
                SELECT course_code
                FROM course_grades
                WHERE grade = 'F'
                  AND cnt * 100.0 / total > 5
            )
            SELECT cg.course_code, cg.title, cg.grade,
                   ROUND(cg.cnt * 100.0 / cg.total, 2) AS percentage
            FROM course_grades cg
            JOIN fail_courses fc ON cg.course_code = fc.course_code
            ORDER BY cg.course_code, cg.grade;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Calculate each student's weighted GPA based on the credits of courses they've taken (using A=4, B=3, C=2, D=1, F=0 scale) and compare it to their recorded GPA.",
        "gold_query": """
            WITH grade_points AS (
                SELECT e.student_id, c.credits,
                       CASE e.grade
                           WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
                           WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
                           WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
                           WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
                       END AS points
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
                JOIN courses c ON s.course_id = c.course_id
                WHERE e.grade IS NOT NULL
            )
            SELECT st.first_name, st.last_name, st.gpa AS recorded_gpa,
                   ROUND(SUM(gp.points * gp.credits) / NULLIF(SUM(gp.credits), 0), 2) AS weighted_gpa,
                   ROUND(st.gpa - SUM(gp.points * gp.credits) / NULLIF(SUM(gp.credits), 0), 2) AS gpa_diff
            FROM students st
            JOIN grade_points gp ON st.student_id = gp.student_id
            WHERE gp.points IS NOT NULL
            GROUP BY st.student_id, st.first_name, st.last_name, st.gpa
            HAVING ABS(st.gpa - SUM(gp.points * gp.credits) / NULLIF(SUM(gp.credits), 0)) > 0.5
            ORDER BY ABS(st.gpa - SUM(gp.points * gp.credits) / NULLIF(SUM(gp.credits), 0)) DESC
            LIMIT 20;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Identify departments where more than 30% of enrolled students have a GPA below the university-wide average.",
        "gold_query": """
            WITH uni_avg AS (
                SELECT AVG(gpa) AS avg_gpa FROM students
            ),
            dept_stats AS (
                SELECT d.name,
                       COUNT(*) AS total_students,
                       COUNT(*) FILTER (WHERE s.gpa < ua.avg_gpa) AS below_avg
                FROM departments d
                JOIN students s ON d.dept_id = s.major_dept_id
                CROSS JOIN uni_avg ua
                GROUP BY d.dept_id, d.name
            )
            SELECT name, total_students, below_avg,
                   ROUND(below_avg * 100.0 / total_students, 2) AS below_avg_pct
            FROM dept_stats
            WHERE below_avg * 100.0 / total_students > 30
            ORDER BY below_avg_pct DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Find faculty members whose students consistently perform above the course average (in more than 60% of their sections).",
        "gold_query": """
            WITH section_avg AS (
                SELECT s.section_id,
                       AVG(CASE e.grade
                           WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
                           WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
                           WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
                           WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
                       END) AS section_gpa
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
                WHERE e.grade IS NOT NULL
                GROUP BY s.section_id
            ),
            course_avg AS (
                SELECT s.course_id,
                       AVG(sa.section_gpa) AS course_gpa
                FROM sections s
                JOIN section_avg sa ON s.section_id = sa.section_id
                GROUP BY s.course_id
            ),
            faculty_performance AS (
                SELECT s.faculty_id, s.section_id,
                       sa.section_gpa, ca.course_gpa,
                       CASE WHEN sa.section_gpa > ca.course_gpa THEN 1 ELSE 0 END AS above_avg
                FROM sections s
                JOIN section_avg sa ON s.section_id = sa.section_id
                JOIN course_avg ca ON s.course_id = ca.course_id
            )
            SELECT f.first_name, f.last_name,
                   COUNT(*) AS total_sections,
                   SUM(above_avg) AS above_avg_sections,
                   ROUND(SUM(above_avg) * 100.0 / COUNT(*), 2) AS above_avg_pct
            FROM faculty_performance fp
            JOIN faculty f ON fp.faculty_id = f.faculty_id
            GROUP BY f.faculty_id, f.first_name, f.last_name
            HAVING SUM(above_avg) * 100.0 / COUNT(*) > 60 AND COUNT(*) >= 2
            ORDER BY above_avg_pct DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Compute the semester-over-semester enrollment growth rate for each department.",
        "gold_query": """
            WITH dept_semester_enrollment AS (
                SELECT c.dept_id, s.semester, s.year,
                       COUNT(e.enrollment_id) AS enrollments,
                       ROW_NUMBER() OVER (PARTITION BY c.dept_id ORDER BY s.year, CASE s.semester WHEN 'Spring' THEN 1 WHEN 'Fall' THEN 2 END) AS sem_order
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
                JOIN courses c ON s.course_id = c.course_id
                GROUP BY c.dept_id, s.semester, s.year
            )
            SELECT d.name, dse.semester, dse.year, dse.enrollments,
                   ROUND((dse.enrollments - LAG(dse.enrollments) OVER (PARTITION BY dse.dept_id ORDER BY dse.sem_order)) * 100.0 /
                         NULLIF(LAG(dse.enrollments) OVER (PARTITION BY dse.dept_id ORDER BY dse.sem_order), 0), 2) AS growth_rate_pct
            FROM dept_semester_enrollment dse
            JOIN departments d ON dse.dept_id = d.dept_id
            ORDER BY d.name, dse.year, dse.semester;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Which students have improved their grade in every subsequent semester they were enrolled (comparing average grades per semester)?",
        "gold_query": """
            WITH student_semester_gpa AS (
                SELECT e.student_id, s.year, s.semester,
                       AVG(CASE e.grade
                           WHEN 'A' THEN 4.0 WHEN 'A-' THEN 3.7
                           WHEN 'B+' THEN 3.3 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
                           WHEN 'C+' THEN 2.3 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
                           WHEN 'D' THEN 1.0 WHEN 'F' THEN 0.0
                       END) AS sem_gpa,
                       ROW_NUMBER() OVER (PARTITION BY e.student_id ORDER BY s.year, CASE s.semester WHEN 'Spring' THEN 1 WHEN 'Fall' THEN 2 END) AS sem_order
                FROM enrollments e
                JOIN sections s ON e.section_id = s.section_id
                WHERE e.grade IS NOT NULL
                GROUP BY e.student_id, s.year, s.semester
            ),
            with_prev AS (
                SELECT student_id, year, semester, sem_gpa,
                       LAG(sem_gpa) OVER (PARTITION BY student_id ORDER BY sem_order) AS prev_gpa,
                       sem_order
                FROM student_semester_gpa
            ),
            consistently_improving AS (
                SELECT student_id,
                       COUNT(*) AS total_semesters,
                       COUNT(*) FILTER (WHERE sem_gpa > prev_gpa) AS improving_semesters
                FROM with_prev
                WHERE prev_gpa IS NOT NULL
                GROUP BY student_id
                HAVING COUNT(*) = COUNT(*) FILTER (WHERE sem_gpa > prev_gpa)
                   AND COUNT(*) >= 2
            )
            SELECT s.first_name, s.last_name, ci.total_semesters + 1 AS semesters_enrolled
            FROM consistently_improving ci
            JOIN students s ON ci.student_id = s.student_id
            ORDER BY ci.total_semesters DESC, s.last_name;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Find the most oversubscribed sections (enrollment count exceeding max_enrollment) and their fill rate.",
        "gold_query": """
            SELECT c.course_code, c.title, s.semester, s.year,
                   c.max_enrollment, COUNT(e.enrollment_id) AS actual_enrollment,
                   ROUND(COUNT(e.enrollment_id) * 100.0 / c.max_enrollment, 2) AS fill_rate_pct
            FROM sections s
            JOIN courses c ON s.course_id = c.course_id
            JOIN enrollments e ON s.section_id = e.section_id
            GROUP BY s.section_id, c.course_id, c.course_code, c.title, s.semester, s.year, c.max_enrollment
            HAVING COUNT(e.enrollment_id) > c.max_enrollment
            ORDER BY fill_rate_pct DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Calculate the budget efficiency per department: budget per student enrolled and budget per course section offered.",
        "gold_query": """
            WITH dept_students AS (
                SELECT major_dept_id AS dept_id, COUNT(*) AS student_count
                FROM students
                GROUP BY major_dept_id
            ),
            dept_sections AS (
                SELECT c.dept_id, COUNT(s.section_id) AS section_count
                FROM courses c
                JOIN sections s ON c.course_id = s.course_id
                GROUP BY c.dept_id
            )
            SELECT d.name, d.budget,
                   COALESCE(ds.student_count, 0) AS students,
                   COALESCE(dsc.section_count, 0) AS sections,
                   ROUND(d.budget / NULLIF(ds.student_count, 0), 2) AS budget_per_student,
                   ROUND(d.budget / NULLIF(dsc.section_count, 0), 2) AS budget_per_section
            FROM departments d
            LEFT JOIN dept_students ds ON d.dept_id = ds.dept_id
            LEFT JOIN dept_sections dsc ON d.dept_id = dsc.dept_id
            ORDER BY d.name;
        """
    },
]
