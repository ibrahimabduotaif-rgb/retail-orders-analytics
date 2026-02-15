-------------------------------------------------------
-- ╔════════════════════════════════════════════════════════════════════════════╗
-- ║   استعلامات تحليل بيانات الطلبات بالتجزئة - نسخة محسّنة       ║
-- ║   Retail Orders Analysis Queries - Improved Version             ║
-- ╟════════════════════════════════════════════════════════════════════════════╝
-- ║   ✅ إصلاح الأخطاء المنطقية                                    ║
-- ║   ✅ توافق مع SQL Server و PostgreSQL و SQLite                  ║
-- ║   ✅ توثيق كامل بالعربي                                        ║
-- ║   ✅ تحسينات في الأداء                                         ║
-- ╚════════════════════════════════════════════════════════════════════════════╝


-- =================================================================
-- 📊 استعلام 1: أعلى 10 منتجات من حيث الإيرادات
-- =================================================================
-- ✅ الأصل: صحيح منطقياً وبنيوياً
-- ✅ تحسين: إضافة عدد الطلبات ومتوسط السعر للسياق

-- النسخة الأصلية (سليمة):
-- SELECT TOP 10 product_id, SUM(sale_price) AS sales
-- FROM df_orders
-- GROUP BY product_id
-- ORDER BY sales DESC

-- النسخة المحسّنة:
SELECT
    product_id,
    SUM(sale_price)                          AS total_revenue,
    COUNT(*)                                 AS total_orders,
    ROUND(AVG(sale_price), 2)                AS avg_order_value,
    SUM(profit)                              AS total_profit,
    ROUND(SUM(profit) / NULLIF(SUM(sale_price), 0) * 100, 1)
                                             AS profit_margin_pct
FROM df_orders
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;  -- استخدم TOP 10 في SQL Server بدل LIMIT

/*
 💡 ملاحظات التحسين:
    - إضافة عدد الطلبات: هل المنتج يبيع كثيراً بسعر منخفض أم قليلاً بسعر عالي؟
    - إضافة هامش الربح: إيرادات عالية لا تعني بالضرورة ربح عالي
    - NULLIF تحمي من القسمة على صفر
*/


-- =================================================================
-- 📊 استعلام 2: أعلى 5 منتجات مبيعاً في كل منطقة
-- =================================================================
-- ✅ الأصل: صحيح - استخدام جيد لـ CTE و ROW_NUMBER
-- ✅ تحسين: إضافة RANK بدل ROW_NUMBER للتعامل مع التعادل

-- النسخة الأصلية (مراجعة):
-- ❓ ROW_NUMBER يتجاهل التعادل: لو منتجان بنفس المبيعات
--    أحدهما يأخذ المركز 5 والآخر 6 بشكل عشوائي
-- ✅ DENSE_RANK يحل هذه المشكلة

WITH ranked_products AS (
    SELECT
        region,
        product_id,
        SUM(sale_price) AS total_sales,
        SUM(profit)     AS total_profit,
        COUNT(*)        AS order_count,
        DENSE_RANK() OVER(
            PARTITION BY region
            ORDER BY SUM(sale_price) DESC
        ) AS sales_rank
    FROM df_orders
    GROUP BY region, product_id
)
SELECT
    region,
    product_id,
    total_sales,
    total_profit,
    order_count,
    sales_rank
FROM ranked_products
WHERE sales_rank <= 5
ORDER BY region, sales_rank;

/*
 💡 ملاحظات:
    - DENSE_RANK vs ROW_NUMBER: لو منتجان متعادلان في المبيعات
      ROW_NUMBER: يعطيهم 4, 5 (عشوائي) → قد نفقد منتج مهم
      DENSE_RANK: يعطيهم 4, 4 ثم 5 → عدالة أكثر
    - إضافة الأداء المجمّع لكل CTE يجنبنا subqueries إضافية
*/


-- =================================================================
-- 📊 استعلام 3: مقارنة النمو الشهري 2022 مقابل 2023
-- =================================================================
-- ✅ الأصل: منطق PIVOT بـ CASE صحيح
-- ✅ تحسين: إضافة حساب نسبة النمو + معالجة القسمة على صفر

WITH monthly_sales AS (
    SELECT
        EXTRACT(YEAR FROM order_date)  AS order_year,  -- SQL Server: YEAR(order_date)
        EXTRACT(MONTH FROM order_date) AS order_month, -- SQL Server: MONTH(order_date)
        SUM(sale_price) AS sales
    FROM df_orders
    WHERE EXTRACT(YEAR FROM order_date) IN (2022, 2023)
    GROUP BY
        EXTRACT(YEAR FROM order_date),
        EXTRACT(MONTH FROM order_date)
)
SELECT
    order_month,
    COALESCE(SUM(CASE WHEN order_year = 2022 THEN sales END), 0) AS sales_2022,
    COALESCE(SUM(CASE WHEN order_year = 2023 THEN sales END), 0) AS sales_2023,

    -- ✅ جديد: حساب النمو المطلق
    COALESCE(SUM(CASE WHEN order_year = 2023 THEN sales END), 0)
    - COALESCE(SUM(CASE WHEN order_year = 2022 THEN sales END), 0)
        AS growth_absolute,

    -- ✅ جديد: حساب نسبة النمو مع حماية من القسمة على صفر
    ROUND(
        (COALESCE(SUM(CASE WHEN order_year = 2023 THEN sales END), 0)
         - COALESCE(SUM(CASE WHEN order_year = 2022 THEN sales END), 0))
        / NULLIF(COALESCE(SUM(CASE WHEN order_year = 2022 THEN sales END), 0), 0)
        * 100
    , 1) AS growth_pct

FROM monthly_sales
GROUP BY order_month
ORDER BY order_month;

/*
 💡 ملاحظات:
    - COALESCE: يحوّل NULL إلى 0 في الأشهر بدون مبيعات
    - NULLIF: يحمي من القسمة على صفر إذا لم تكن هناك مبيعات في 2022
    - إضافة WHERE للتصفية المبكّرة تحسّن الأداء
    - ❌ في الأصل: لو كان شهر موجود في 2023 وغير موجود في 2022
      يظهر NULL بدل 0 — وهذا مُضلّل
*/


-- =================================================================
-- 📊 استعلام 4: أعلى شهر مبيعات لكل فئة
-- =================================================================
-- ✅ الأصل: صحيح - استخدام FORMAT جيد
-- ✅ تحسين: إضافة سياق (ترتيب عام + نسبة من إجمالي الفئة)

WITH category_monthly AS (
    SELECT
        category,
        -- SQL Server: FORMAT(order_date, 'yyyyMM')
        -- PostgreSQL/SQLite:
        STRFTIME('%Y%m', order_date) AS year_month,
        SUM(sale_price) AS monthly_sales,
        SUM(profit)     AS monthly_profit,
        COUNT(*)        AS order_count
    FROM df_orders
    GROUP BY category, STRFTIME('%Y%m', order_date)
),
category_totals AS (
    SELECT
        category,
        SUM(monthly_sales) AS category_total_sales
    FROM category_monthly
    GROUP BY category
),
ranked AS (
    SELECT
        cm.*,
        ct.category_total_sales,
        ROUND(cm.monthly_sales / ct.category_total_sales * 100, 1)
            AS pct_of_category_total,
        ROW_NUMBER() OVER(
            PARTITION BY cm.category
            ORDER BY cm.monthly_sales DESC
        ) AS rn
    FROM category_monthly cm
    JOIN category_totals ct ON cm.category = ct.category
)
SELECT
    category,
    year_month          AS peak_month,
    monthly_sales       AS peak_sales,
    monthly_profit      AS peak_profit,
    order_count         AS peak_orders,
    pct_of_category_total
FROM ranked
WHERE rn = 1
ORDER BY monthly_sales DESC;

/*
 💡 ملاحظات:
    - النسبة من إجمالي الفئة تُظهر مدى تركّز المبيعات
      مثلاً: لو شهر واحد يمثل 40% من مبيعات الفئة = موسمية عالية
    - هذا مفيد لتخطيط المخزون والحملات التسويقية
*/


-- =================================================================
-- 📊 استعلام 5: أعلى فئة فرعية نمواً في الربح (2022 → 2023)
-- =================================================================
/*
 ❌ خطأ منطقي في الأصل:
    العنوان يقول "highest growth by PROFIT"
    لكن الكود يستخدم SUM(sale_price) بدل SUM(profit)!
    هذا خطأ جوهري يغيّر النتائج بالكامل.

 ✅ التحسين: إصلاح الخطأ + إضافة نسبة النمو + إظهار الأعلى والأدنى
*/

WITH subcategory_yearly AS (
    SELECT
        sub_category,
        EXTRACT(YEAR FROM order_date) AS order_year,
        SUM(profit)     AS total_profit,     -- ✅ تم الإصلاح: profit بدل sale_price
        SUM(sale_price)  AS total_sales
    FROM df_orders
    WHERE EXTRACT(YEAR FROM order_date) IN (2022, 2023)
    GROUP BY sub_category, EXTRACT(YEAR FROM order_date)
),
pivoted AS (
    SELECT
        sub_category,
        COALESCE(SUM(CASE WHEN order_year = 2022 THEN total_profit END), 0)
            AS profit_2022,
        COALESCE(SUM(CASE WHEN order_year = 2023 THEN total_profit END), 0)
            AS profit_2023,
        COALESCE(SUM(CASE WHEN order_year = 2022 THEN total_sales END), 0)
            AS sales_2022,
        COALESCE(SUM(CASE WHEN order_year = 2023 THEN total_sales END), 0)
            AS sales_2023
    FROM subcategory_yearly
    GROUP BY sub_category
)
SELECT
    sub_category,
    profit_2022,
    profit_2023,
    profit_2023 - profit_2022 AS profit_growth_absolute,
    ROUND(
        (profit_2023 - profit_2022)
        / NULLIF(ABS(profit_2022), 0) * 100
    , 1) AS profit_growth_pct,
    sales_2022,
    sales_2023
FROM pivoted
ORDER BY profit_growth_absolute DESC;
-- أزل LIMIT 1 لرؤية الصورة الكاملة، أو أضفه لأعلى نتيجة فقط

/*
 💡 ملاحظات:
    - ABS في المقام: لو الربح في 2022 كان سالباً والآن إيجابي
      بدون ABS ستكون النسبة سالبة (مُضلّلة)
    - إظهار كل الفئات الفرعية أفضل من TOP 1
      لأنه يتيح رؤية الأنماط الكاملة (من يصعد ومن ينزل)
    - إضافة المبيعات بجانب الأرباح تعطي سياق أعمق
*/


-- =================================================================
-- 📊 استعلامات إضافية مقترحة (غير موجودة في الأصل)
-- =================================================================

-- 📊 6: تحليل الموسمية - أي يوم في الأسبوع أكثر مبيعات؟
SELECT
    CASE STRFTIME('%w', order_date)
        WHEN '0' THEN 'الأحد'
        WHEN '1' THEN 'الإثنين'
        WHEN '2' THEN 'الثلاثاء'
        WHEN '3' THEN 'الأربعاء'
        WHEN '4' THEN 'الخميس'
        WHEN '5' THEN 'الجمعة'
        WHEN '6' THEN 'السبت'
    END AS day_name,
    COUNT(*) AS total_orders,
    ROUND(AVG(sale_price), 2) AS avg_sale,
    SUM(profit) AS total_profit
FROM df_orders
GROUP BY STRFTIME('%w', order_date)
ORDER BY total_orders DESC;


-- 📊 7: تحليل Ship Mode - أثر طريقة الشحن على الأداء
SELECT
    ship_mode,
    COUNT(*) AS order_count,
    ROUND(AVG(sale_price), 2) AS avg_sale,
    SUM(profit) AS total_profit,
    ROUND(SUM(profit) / NULLIF(SUM(sale_price), 0) * 100, 1)
        AS profit_margin_pct
FROM df_orders
GROUP BY ship_mode
ORDER BY total_profit DESC;


-- 📊 8: تحليل RFM مبسّط (Recency, Frequency, Monetary)
WITH customer_metrics AS (
    SELECT
        customer_id,  -- إذا كان العمود موجوداً في البيانات
        MAX(order_date) AS last_order_date,
        COUNT(*) AS total_orders,
        SUM(sale_price) AS total_spent,
        SUM(profit) AS total_profit
    FROM df_orders
    GROUP BY customer_id
)
SELECT
    *,
    NTILE(4) OVER (ORDER BY last_order_date DESC) AS recency_quartile,
    NTILE(4) OVER (ORDER BY total_orders ASC) AS frequency_quartile,
    NTILE(4) OVER (ORDER BY total_spent ASC) AS monetary_quartile
FROM customer_metrics
ORDER BY total_spent DESC
LIMIT 20;
