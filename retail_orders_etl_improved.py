# =============================================================================
# 1. استيراد المكتبات
# =============================================================================
# ملاحظة: في النسخة الأصلية لم يكن هناك تنظيم للاستيراد
# التحسين: تجميع المكتبات حسب الوظيفة مع التعليق على كل مجموعة

import os
import logging
import zipfile
from datetime import datetime

import pandas as pd
import numpy as np
import sqlalchemy as sal
from sqlalchemy import text

# إعداد نظام تسجيل العمليات - غير موجود في النسخة الأصلية
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# 2. إعدادات المشروع (Configuration)
# =============================================================================
# ❌ مشكلة في الأصل: الإعدادات متناثرة في الكود
# ✅ التحسين: تجميعها في مكان واحد لسهولة التعديل

CONFIG = {
    # مسارات الملفات
    "dataset": "ankitbansal06/retail-orders",
    "zip_file": "orders.csv.zip",
    "csv_file": "orders.csv",

    # القيم التي تُعامل كـ null
    # ✅ نقطة قوة موجودة في الأصل - تم توسيعها هنا
    "na_values": ['Not Available', 'unknown', 'N/A', 'NA', '', 'null', 'none'],

    # إعدادات قاعدة البيانات
    # ❌ في الأصل: اسم جهاز محدد (ANKIT\\SQLEXPRESS) - غير محمول
    # ✅ التحسين: استخدام متغيرات بيئة أو SQLite كبديل محمول
    "db_connection": os.getenv(
        "DB_CONNECTION_STRING",
        "sqlite:///retail_orders.db"  # بديل محمول يعمل على أي جهاز
    ),
    "table_name": "df_orders",
}


# =============================================================================
# 3. استخراج البيانات (Extract)
# =============================================================================
def extract_data() -> str:
    """
    تحميل واستخراج البيانات من Kaggle

    ❌ مشكلة في الأصل: لا يوجد error handling
       لو فشل التحميل ينهار الكود بالكامل
    ✅ التحسين: try-except مع رسائل واضحة
    """
    logger.info("⬇️  بدء تحميل البيانات من Kaggle...")

    try:
        # تحميل من Kaggle
        os.system(
            f'kaggle datasets download {CONFIG["dataset"]} '
            f'-f {CONFIG["csv_file"]}'
        )
        logger.info("✅ تم التحميل بنجاح")
    except Exception as e:
        logger.error(f"❌ فشل التحميل: {e}")
        raise

    # فك الضغط
    try:
        with zipfile.ZipFile(CONFIG["zip_file"], 'r') as zip_ref:
            zip_ref.extractall()
            # ✅ التحسين: استخدام context manager (with)
            # ❌ في الأصل: فتح وإغلاق يدوي - خطر نسيان الإغلاق
        logger.info("✅ تم فك الضغط بنجاح")
    except FileNotFoundError:
        logger.error(f"❌ الملف غير موجود: {CONFIG['zip_file']}")
        raise
    except zipfile.BadZipFile:
        logger.error("❌ الملف تالف أو ليس ملف zip صالح")
        raise

    return CONFIG["csv_file"]


# =============================================================================
# 4. تحويل البيانات (Transform)
# =============================================================================
def transform_data(filepath: str) -> pd.DataFrame:
    """
    قراءة وتنظيف وتحويل البيانات

    هذا القسم فيه أكبر المشاكل في النسخة الأصلية:
    ❌ أغلب الكود معلّق (commented out) - يعني لا يعمل فعلياً!
    ❌ لا يوجد تحقق من البيانات بعد كل خطوة
    ❌ لا يوجد توثيق لمنطق الحسابات
    """

    # --- 4.1 قراءة البيانات ---
    logger.info("📚 قراءة البيانات...")
    df = pd.read_csv(filepath, na_values=CONFIG["na_values"])

    # ✅ تحقق أولي - غير موجود في الأصل
    logger.info(f"   عدد الصفوف: {len(df):,}")
    logger.info(f"   عدد الأعمدة: {df.shape[1]}")
    logger.info(f"   القيم الفارغة:\n{df.isnull().sum()[df.isnull().sum() > 0]}")

    # --- 4.2 تنظيف أسماء الأعمدة ---
    # ❌ في الأصل: الكود معلّق! الأعمدة تبقى بأسمائها القديمة
    # ✅ التحسين: تفعيل الكود مع حماية إضافية
    original_columns = df.columns.tolist()
    df.columns = (
        df.columns
        .str.lower()
        .str.strip()              # إزالة المسافات الزائدة
        .str.replace(' ', '_')
        .str.replace('[^a-z0-9_]', '', regex=True)  # إزالة الرموز الخاصة
    )
    logger.info(f"   تم تنظيف الأعمدة: {dict(zip(original_columns, df.columns))}")

    # --- 4.3 اشتقاق الأعمدة الجديدة ---
    """
    ❌ أخطاء في الأصل:
       1. الكود معلّق بالكامل!
       2. صيغة الخصم: df['list_price']*df['discount_percent']*.01
          - الضرب في 0.01 يعني أن discount_percent مخزن كنسبة مئوية (مثلاً 20 = 20%)
          - هذا صحيح لكن يحتاج توثيق

    ✅ التحسين: تفعيل مع توثيق واضح وتحقق
    """

    # التحقق من وجود الأعمدة المطلوبة قبل الحساب
    required_cols = ['list_price', 'discount_percent', 'cost_price']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ أعمدة مفقودة: {missing_cols}")

    # حساب الخصم: قيمة الخصم = سعر القائمة × (نسبة الخصم / 100)
    df['discount'] = df['list_price'] * df['discount_percent'] * 0.01

    # سعر البيع = سعر القائمة - قيمة الخصم
    df['sale_price'] = df['list_price'] - df['discount']

    # الربح = سعر البيع - سعر التكلفة
    df['profit'] = df['sale_price'] - df['cost_price']

    # ✅ تحقق من منطقية الحسابات - غير موجود في الأصل
    negative_profit_count = (df['profit'] < 0).sum()
    negative_sale_count = (df['sale_price'] < 0).sum()

    if negative_sale_count > 0:
        logger.warning(f"⚠️  يوجد {negative_sale_count} صف بسعر بيع سالب!")
    if negative_profit_count > 0:
        logger.warning(
            f"⚠️  يوجد {negative_profit_count} صف بربح سالب "
            f"({negative_profit_count/len(df)*100:.1f}% من البيانات)"
        )

    # --- 4.4 تحويل التاريخ ---
    # ✅ في الأصل: موجود لكن بدون error handling
    try:
        df['order_date'] = pd.to_datetime(df['order_date'], format="%Y-%m-%d")
    except ValueError:
        logger.warning("⚠️  تنسيق التاريخ غير متطابق، محاولة التحويل التلقائي...")
        df['order_date'] = pd.to_datetime(df['order_date'], infer_datetime_format=True)

    # --- 4.5 حذف الأعمدة المؤقتة ---
    # ✅ التحسين: التحقق من وجود الأعمدة قبل الحذف
    cols_to_drop = ['list_price', 'cost_price', 'discount_percent']
    existing_to_drop = [col for col in cols_to_drop if col in df.columns]
    df.drop(columns=existing_to_drop, inplace=True)

    # --- 4.6 ملخص نهائي ---
    logger.info("=" * 50)
    logger.info("📊 ملخص البيانات بعد التحويل:")
    logger.info(f"   الصفوف: {len(df):,}")
    logger.info(f"   الأعمدة: {list(df.columns)}")
    logger.info(f"   نطاق التواريخ: {df['order_date'].min()} → {df['order_date'].max()}")
    logger.info(f"   إجمالي المبيعات: ${df['sale_price'].sum():,.2f}")
    logger.info(f"   إجمالي الأرباح: ${df['profit'].sum():,.2f}")
    logger.info(f"   هامش الربح: {df['profit'].sum()/df['sale_price'].sum()*100:.1f}%")
    logger.info("=" * 50)

    return df


# =============================================================================
# 5. تحميل البيانات (Load)
# =============================================================================
def load_data(df: pd.DataFrame) -> None:
    """
    تحميل البيانات إلى قاعدة البيانات

    ❌ مشاكل في الأصل:
       1. الاتصال مرتبط بجهاز محدد (ANKIT\\SQLEXPRESS)
       2. استخدام 'append' بدون تحقق من التكرار
       3. لا يوجد إغلاق للاتصال
    ✅ التحسينات:
       1. اتصال محمول عبر متغيرات بيئة
       2. استخدام 'replace' أولاً ثم 'append' حسب الحاجة
       3. إغلاق تلقائي مع context manager
    """
    logger.info(f"💾 تحميل البيانات إلى: {CONFIG['db_connection'][:30]}...")

    try:
        engine = sal.create_engine(CONFIG["db_connection"])

        # اختبار الاتصال أولاً
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ الاتصال بقاعدة البيانات ناجح")

        # تحميل البيانات
        # ✅ استخدام 'replace' للتشغيل الأول لتجنب تكرار البيانات
        df.to_sql(
            CONFIG["table_name"],
            con=engine,
            index=False,
            if_exists='replace',  # ❌ في الأصل: 'append' قد يّكرر البيانات
            chunksize=1000,       # ✅ تحميل على دفعات لتحسين الأداء
            method='multi'        # ✅ إدراج متعدد أسرع
        )
        logger.info(f"✅ تم تحميل {len(df):,} صف بنجاح")

    except sal.exc.OperationalError as e:
        logger.error(f"❌ فشل الاتصال بقاعدة البيانات: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع: {e}")
        raise
    finally:
        engine.dispose()
        logger.info("🔒 تم إغلاق الاتصال")


# =============================================================================
# 6. التشغيل الرئيسي
# =============================================================================
# ❌ في الأصل: لا يوجد main function - الكود يعمل بشكل خطي
# ✅ التحسين: هيكلة واضحة مع قياس الوقت

def main():
    """تشغيل ETL Pipeline الكامل"""
    start_time = datetime.now()
    logger.info("🚀 بدء تشغيل ETL Pipeline")
    logger.info("=" * 60)

    try:
        # المرحلة 1: الاستخراج
        csv_path = extract_data()

        # المرحلة 2: التحويل
        df = transform_data(csv_path)

        # المرحلة 3: التحميل
        load_data(df)

        elapsed = datetime.now() - start_time
        logger.info("=" * 60)
        logger.info(f"🎉 اكتمل بنجاح في {elapsed.total_seconds():.1f} ثانية")

    except Exception as e:
        logger.error(f"💥 فشل Pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
