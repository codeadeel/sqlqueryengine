#!/usr/bin/env python3

# %%
# E-commerce evaluation questions — 40 questions across 4 difficulty tiers
# 10 easy, 12 medium, 10 hard, 8 extra hard

ECOMMERCE_QUESTIONS = [
    # Easy (10)
    {
        "difficulty": "easy",
        "question": "How many customers are there?",
        "gold_query": "SELECT COUNT(*) FROM customers;"
    },
    {
        "difficulty": "easy",
        "question": "What is the most expensive product?",
        "gold_query": "SELECT name, price FROM products ORDER BY price DESC LIMIT 1;"
    },
    {
        "difficulty": "easy",
        "question": "How many orders have been delivered?",
        "gold_query": "SELECT COUNT(*) FROM orders WHERE status = 'delivered';"
    },
    {
        "difficulty": "easy",
        "question": "List all product categories that have no parent category.",
        "gold_query": "SELECT name FROM categories WHERE parent_category_id IS NULL ORDER BY name;"
    },
    {
        "difficulty": "easy",
        "question": "What is the average product price?",
        "gold_query": "SELECT ROUND(AVG(price), 2) FROM products;"
    },
    {
        "difficulty": "easy",
        "question": "How many premium customers are there?",
        "gold_query": "SELECT COUNT(*) FROM customers WHERE is_premium = TRUE;"
    },
    {
        "difficulty": "easy",
        "question": "What is the total revenue from all orders?",
        "gold_query": "SELECT ROUND(SUM(total_amount), 2) FROM orders;"
    },
    {
        "difficulty": "easy",
        "question": "How many products are currently inactive?",
        "gold_query": "SELECT COUNT(*) FROM products WHERE is_active = FALSE;"
    },
    {
        "difficulty": "easy",
        "question": "What is the average review rating across all reviews?",
        "gold_query": "SELECT ROUND(AVG(rating), 2) FROM reviews;"
    },
    {
        "difficulty": "easy",
        "question": "How many different countries do customers come from?",
        "gold_query": "SELECT COUNT(DISTINCT country) FROM customers;"
    },

    # Medium (12)
    {
        "difficulty": "medium",
        "question": "Which customer has placed the most orders?",
        "gold_query": "SELECT c.first_name, c.last_name, COUNT(o.order_id) AS order_count FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name, c.last_name ORDER BY order_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average order total for each order status?",
        "gold_query": "SELECT status, ROUND(AVG(total_amount), 2) AS avg_total FROM orders GROUP BY status ORDER BY avg_total DESC;"
    },
    {
        "difficulty": "medium",
        "question": "Which product has the highest average review rating with at least 3 reviews?",
        "gold_query": "SELECT p.name, ROUND(AVG(r.rating), 2) AS avg_rating, COUNT(r.review_id) AS review_count FROM products p JOIN reviews r ON p.product_id = r.product_id GROUP BY p.product_id, p.name HAVING COUNT(r.review_id) >= 3 ORDER BY avg_rating DESC, review_count DESC, p.product_id LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "How many orders were placed in each month of 2025?",
        "gold_query": "SELECT EXTRACT(MONTH FROM order_date)::INTEGER AS month, COUNT(*) AS order_count FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2025 GROUP BY EXTRACT(MONTH FROM order_date) ORDER BY month;"
    },
    {
        "difficulty": "medium",
        "question": "What is the total revenue per country?",
        "gold_query": "SELECT c.country, ROUND(SUM(o.total_amount), 2) AS total_revenue FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.country ORDER BY total_revenue DESC;"
    },
    {
        "difficulty": "medium",
        "question": "Which payment method is used most frequently?",
        "gold_query": "SELECT payment_method, COUNT(*) AS usage_count FROM payments GROUP BY payment_method ORDER BY usage_count DESC LIMIT 1;"
    },
    {
        "difficulty": "medium",
        "question": "What are the top 5 products by total quantity sold?",
        "gold_query": "SELECT p.name, SUM(oi.quantity) AS total_sold FROM products p JOIN order_items oi ON p.product_id = oi.product_id GROUP BY p.product_id, p.name ORDER BY total_sold DESC, p.product_id LIMIT 5;"
    },
    {
        "difficulty": "medium",
        "question": "How many customers have never placed an order?",
        "gold_query": "SELECT COUNT(*) FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_id IS NULL;"
    },
    {
        "difficulty": "medium",
        "question": "What is the average number of items per order?",
        "gold_query": "SELECT ROUND(AVG(item_count), 2) FROM (SELECT order_id, COUNT(*) AS item_count FROM order_items GROUP BY order_id) sub;"
    },
    {
        "difficulty": "medium",
        "question": "Which categories have products with an average price above 200?",
        "gold_query": "SELECT cat.name, ROUND(AVG(p.price), 2) AS avg_price FROM categories cat JOIN products p ON cat.category_id = p.category_id GROUP BY cat.category_id, cat.name HAVING AVG(p.price) > 200 ORDER BY avg_price DESC;"
    },
    {
        "difficulty": "medium",
        "question": "What percentage of orders have been cancelled?",
        "gold_query": "SELECT ROUND(COUNT(*) FILTER (WHERE status = 'cancelled') * 100.0 / COUNT(*), 2) AS cancel_pct FROM orders;"
    },
    {
        "difficulty": "medium",
        "question": "List customers who have spent more than 5000 in total.",
        "gold_query": "SELECT c.first_name, c.last_name, ROUND(SUM(o.total_amount), 2) AS total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name, c.last_name HAVING SUM(o.total_amount) > 5000 ORDER BY total_spent DESC;"
    },

    # Hard (10)
    {
        "difficulty": "hard",
        "question": "For each month in 2025, what was the revenue and how did it compare to the previous month as a percentage change?",
        "gold_query": """
            WITH monthly AS (
                SELECT EXTRACT(MONTH FROM order_date)::INTEGER AS month,
                       SUM(total_amount) AS revenue
                FROM orders
                WHERE EXTRACT(YEAR FROM order_date) = 2025
                GROUP BY EXTRACT(MONTH FROM order_date)
            )
            SELECT month, ROUND(revenue, 2) AS revenue,
                   ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) / NULLIF(LAG(revenue) OVER (ORDER BY month), 0) * 100, 2) AS pct_change
            FROM monthly
            ORDER BY month;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the rank of each product category by total revenue?",
        "gold_query": """
            SELECT cat.name,
                   ROUND(SUM(oi.quantity * oi.unit_price), 2) AS total_revenue,
                   RANK() OVER (ORDER BY SUM(oi.quantity * oi.unit_price) DESC) AS revenue_rank
            FROM categories cat
            JOIN products p ON cat.category_id = p.category_id
            JOIN order_items oi ON p.product_id = oi.product_id
            GROUP BY cat.category_id, cat.name
            ORDER BY revenue_rank;
        """
    },
    {
        "difficulty": "hard",
        "question": "Which customers have placed orders in every quarter of 2025?",
        "gold_query": """
            SELECT c.first_name, c.last_name
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE EXTRACT(YEAR FROM o.order_date) = 2025
            GROUP BY c.customer_id, c.first_name, c.last_name
            HAVING COUNT(DISTINCT EXTRACT(QUARTER FROM o.order_date)) = 4;
        """
    },
    {
        "difficulty": "hard",
        "question": "Find products that have never been ordered but have at least one review.",
        "gold_query": """
            SELECT p.name
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            JOIN reviews r ON p.product_id = r.product_id
            WHERE oi.item_id IS NULL
            GROUP BY p.product_id, p.name
            ORDER BY p.name;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the running total of revenue by order date for each customer who has spent over 3000?",
        "gold_query": """
            WITH big_spenders AS (
                SELECT customer_id
                FROM orders
                GROUP BY customer_id
                HAVING SUM(total_amount) > 3000
            )
            SELECT c.first_name, c.last_name, o.order_date::date,
                   ROUND(SUM(o.total_amount) OVER (PARTITION BY o.customer_id ORDER BY o.order_date), 2) AS running_total
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN big_spenders bs ON o.customer_id = bs.customer_id
            ORDER BY c.last_name, c.first_name, o.order_date;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the median order total amount?",
        "gold_query": """
            SELECT ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_amount)::NUMERIC, 2) AS median_total
            FROM orders;
        """
    },
    {
        "difficulty": "hard",
        "question": "For each category, what percentage of its products have at least one review?",
        "gold_query": """
            SELECT cat.name,
                   ROUND(COUNT(DISTINCT r.product_id) * 100.0 / NULLIF(COUNT(DISTINCT p.product_id), 0), 2) AS reviewed_pct
            FROM categories cat
            JOIN products p ON cat.category_id = p.category_id
            LEFT JOIN reviews r ON p.product_id = r.product_id
            GROUP BY cat.category_id, cat.name
            ORDER BY reviewed_pct DESC;
        """
    },
    {
        "difficulty": "hard",
        "question": "Which customers have both the highest and lowest rated reviews (gave at least one 5-star and one 1-star review)?",
        "gold_query": """
            SELECT c.first_name, c.last_name
            FROM customers c
            JOIN reviews r ON c.customer_id = r.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            HAVING MIN(r.rating) = 1 AND MAX(r.rating) = 5
            ORDER BY c.last_name, c.first_name;
        """
    },
    {
        "difficulty": "hard",
        "question": "Show the top 3 customers by number of orders for each country.",
        "gold_query": """
            WITH ranked AS (
                SELECT c.country, c.first_name, c.last_name,
                       COUNT(o.order_id) AS order_count,
                       ROW_NUMBER() OVER (PARTITION BY c.country ORDER BY COUNT(o.order_id) DESC) AS rn
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.country, c.customer_id, c.first_name, c.last_name
            )
            SELECT country, first_name, last_name, order_count
            FROM ranked
            WHERE rn <= 3
            ORDER BY country, rn;
        """
    },
    {
        "difficulty": "hard",
        "question": "What is the average time between a customer signing up and placing their first order?",
        "gold_query": """
            WITH first_orders AS (
                SELECT customer_id, MIN(order_date) AS first_order_date
                FROM orders
                GROUP BY customer_id
            )
            SELECT ROUND(AVG(EXTRACT(EPOCH FROM (fo.first_order_date - c.signup_date::timestamp)) / 86400)::NUMERIC, 2) AS avg_days
            FROM customers c
            JOIN first_orders fo ON c.customer_id = fo.customer_id;
        """
    },

    # Extra Hard (8)
    {
        "difficulty": "extra_hard",
        "question": "Find customers whose total spending is above the 90th percentile and who have never left a review below 3 stars.",
        "gold_query": """
            WITH spending AS (
                SELECT customer_id, SUM(total_amount) AS total_spent
                FROM orders
                GROUP BY customer_id
            ),
            p90 AS (
                SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_spent) AS threshold
                FROM spending
            )
            SELECT c.first_name, c.last_name, ROUND(s.total_spent, 2) AS total_spent
            FROM spending s
            JOIN customers c ON s.customer_id = c.customer_id
            CROSS JOIN p90
            WHERE s.total_spent > p90.threshold
              AND NOT EXISTS (
                  SELECT 1 FROM reviews r
                  WHERE r.customer_id = c.customer_id AND r.rating < 3
              )
            ORDER BY s.total_spent DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "For each product, calculate the month-over-month growth rate in order quantity and identify products with consistent growth (positive growth every month) in 2025.",
        "gold_query": """
            WITH monthly_qty AS (
                SELECT p.product_id, p.name,
                       EXTRACT(MONTH FROM o.order_date)::INTEGER AS month,
                       SUM(oi.quantity) AS qty
                FROM products p
                JOIN order_items oi ON p.product_id = oi.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE EXTRACT(YEAR FROM o.order_date) = 2025
                GROUP BY p.product_id, p.name, EXTRACT(MONTH FROM o.order_date)
            ),
            with_growth AS (
                SELECT product_id, name, month, qty,
                       qty - LAG(qty) OVER (PARTITION BY product_id ORDER BY month) AS growth
                FROM monthly_qty
            ),
            months_per_product AS (
                SELECT product_id, name, COUNT(*) AS total_months,
                       COUNT(*) FILTER (WHERE growth > 0) AS positive_months
                FROM with_growth
                GROUP BY product_id, name
            )
            SELECT name, total_months, positive_months
            FROM months_per_product
            WHERE positive_months = total_months - 1 AND total_months >= 3
            ORDER BY name;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Identify category pairs that are frequently bought together in the same order (appear together in at least 5 orders).",
        "gold_query": """
            WITH order_categories AS (
                SELECT DISTINCT oi.order_id, p.category_id
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
            )
            SELECT c1.name AS category_1, c2.name AS category_2, COUNT(*) AS co_occurrence
            FROM order_categories oc1
            JOIN order_categories oc2 ON oc1.order_id = oc2.order_id AND oc1.category_id < oc2.category_id
            JOIN categories c1 ON oc1.category_id = c1.category_id
            JOIN categories c2 ON oc2.category_id = c2.category_id
            GROUP BY c1.category_id, c1.name, c2.category_id, c2.name
            HAVING COUNT(*) >= 5
            ORDER BY co_occurrence DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Calculate the customer lifetime value (total spending / months since signup) for all customers who signed up more than 6 months ago, and show the top 10.",
        "gold_query": """
            SELECT c.first_name, c.last_name,
                   ROUND(SUM(o.total_amount), 2) AS total_spent,
                   EXTRACT(EPOCH FROM ('2025-06-01'::timestamp - c.signup_date::timestamp)) / (86400 * 30) AS months_active,
                   ROUND((SUM(o.total_amount) / NULLIF(EXTRACT(EPOCH FROM ('2025-06-01'::timestamp - c.signup_date::timestamp)) / (86400 * 30), 0))::NUMERIC, 2) AS monthly_clv
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE c.signup_date < '2025-06-01'::date - INTERVAL '6 months'
            GROUP BY c.customer_id, c.first_name, c.last_name, c.signup_date
            ORDER BY monthly_clv DESC
            LIMIT 10;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Find products where the average rating has decreased compared to their first half of reviews vs second half of reviews.",
        "gold_query": """
            WITH numbered AS (
                SELECT product_id, rating,
                       ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY created_at) AS rn,
                       COUNT(*) OVER (PARTITION BY product_id) AS total
                FROM reviews
            ),
            halves AS (
                SELECT product_id,
                       AVG(CASE WHEN rn <= total / 2 THEN rating END) AS first_half_avg,
                       AVG(CASE WHEN rn > total / 2 THEN rating END) AS second_half_avg
                FROM numbered
                WHERE total >= 4
                GROUP BY product_id
            )
            SELECT p.name, ROUND(h.first_half_avg, 2) AS first_half, ROUND(h.second_half_avg, 2) AS second_half
            FROM halves h
            JOIN products p ON h.product_id = p.product_id
            WHERE h.second_half_avg < h.first_half_avg
            ORDER BY (h.first_half_avg - h.second_half_avg) DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "What is the conversion rate from signup to first purchase for each signup month, and which month had the highest conversion rate?",
        "gold_query": """
            WITH monthly_signups AS (
                SELECT DATE_TRUNC('month', signup_date)::date AS signup_month,
                       COUNT(*) AS total_signups
                FROM customers
                GROUP BY DATE_TRUNC('month', signup_date)
            ),
            monthly_converters AS (
                SELECT DATE_TRUNC('month', c.signup_date)::date AS signup_month,
                       COUNT(DISTINCT c.customer_id) AS converters
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY DATE_TRUNC('month', c.signup_date)
            )
            SELECT ms.signup_month, ms.total_signups, COALESCE(mc.converters, 0) AS converters,
                   ROUND(COALESCE(mc.converters, 0) * 100.0 / ms.total_signups, 2) AS conversion_rate
            FROM monthly_signups ms
            LEFT JOIN monthly_converters mc ON ms.signup_month = mc.signup_month
            ORDER BY conversion_rate DESC;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Identify customers who placed an order within 7 days of leaving a negative review (rating <= 2) on any product.",
        "gold_query": """
            SELECT DISTINCT c.first_name, c.last_name
            FROM customers c
            JOIN reviews r ON c.customer_id = r.customer_id
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE r.rating <= 2
              AND o.order_date BETWEEN r.created_at AND r.created_at + INTERVAL '7 days'
            ORDER BY c.last_name, c.first_name;
        """
    },
    {
        "difficulty": "extra_hard",
        "question": "Calculate the Pareto distribution: what percentage of customers account for 80% of total revenue?",
        "gold_query": """
            WITH customer_revenue AS (
                SELECT customer_id, SUM(total_amount) AS revenue
                FROM orders
                GROUP BY customer_id
            ),
            total AS (
                SELECT SUM(revenue) AS total_rev FROM customer_revenue
            ),
            ranked AS (
                SELECT cr.customer_id, cr.revenue,
                       SUM(cr.revenue) OVER (ORDER BY cr.revenue DESC) AS cumulative_rev,
                       ROW_NUMBER() OVER (ORDER BY cr.revenue DESC) AS rn
                FROM customer_revenue cr
            )
            SELECT ROUND(MIN(rn) * 100.0 / (SELECT COUNT(*) FROM customer_revenue), 2) AS pct_customers
            FROM ranked
            CROSS JOIN total
            WHERE cumulative_rev >= total.total_rev * 0.8;
        """
    },
]
