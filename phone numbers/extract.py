import sqlite3
import csv
import os
import openpyxl
from pathlib import Path

def create_sqlite_from_csv(csv_file_path, db_name, table_name):
    # 1. الاتصال بقاعدة البيانات (سيتم إنشاؤها إذا لم تكن موجودة)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    try:
        # 2. فتح ملف CSV مع تحديد الترميز الصحيح للعربية والفاصلة المنقوطة
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as f:
            # نحدد delimiter=';' لأن الإكسل العربي يستخدمها
            reader = csv.reader(f, delimiter=';')
            
            # قراءة الصف الأول (العناوين)
            headers = next(reader)
            
            # 3. تنظيف أسماء الأعمدة (إزالة المسافات والرموز لتكون متوافقة مع SQL)
            clean_headers = []
            for h in headers:
                clean_h = h.strip().replace('/', '_').replace(' ', '_').replace(';', '_')
                if not clean_h: clean_h = "column_unknown"
                clean_headers.append(clean_h)

            # 4. إنشاء جدول بناءً على الأسماء المنظفة
            # سنفترض أن كل الأعمدة نصوص (TEXT) للتبسيط
            cols_query = ", ".join([f'"{name}" TEXT' for name in clean_headers])
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(f"CREATE TABLE {table_name} ({cols_query})")

            # 5. إدخال البيانات
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in clean_headers])})"
            
            count = 0
            for row in reader:
                if any(row):  # التأكد من أن السطر ليس فارغاً
                    cursor.execute(insert_query, row)
                    count += 1

            conn.commit()
            print(f"✅ تم بنجاح! تم إنشاء القاعدة: {db_name}")
            print(f"✅ تم استيراد {count} سجل إلى الجدول: {table_name}")
            print(f"✅ أسماء الأعمدة الجديدة: {', '.join(clean_headers)}")

    except Exception as e:
        print(f"❌ حدث خطأ: {e}")
    finally:
        conn.close()

def create_sqlite_from_xlsx(xlsx_file_path, db_name):
    """تحويل ملف XLSX إلى قاعدة بيانات SQLite مع دعم جداول متعددة"""
    try:
        workbook = openpyxl.load_workbook(xlsx_file_path)
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        
        print(f"\n📄 معالجة الملف: {xlsx_file_path}")
        print(f"   أوراق العمل المتوفرة: {workbook.sheetnames}")
        
        for sheet_name in workbook.sheetnames:
            worksheet = workbook[sheet_name]
            
            # استخراج البيانات والعناوين
            rows = list(worksheet.iter_rows(values_only=True))
            if not rows:
                continue
            
            headers = [str(h) if h else "column_unknown" for h in rows[0]]
            
            # تنظيف أسماء الأعمدة
            clean_headers = []
            for h in headers:
                clean_h = h.strip().replace('/', '_').replace(' ', '_').replace(';', '_')
                if not clean_h: clean_h = "column_unknown"
                clean_headers.append(clean_h)
            
            # إنشاء اسم الجدول من اسم ورقة العمل
            table_name = "company_phones"  # استخدام اسم الجدول بالإنجليزية
            
            # إنشاء الجدول بالهيكل المحدد
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            cursor.execute(f"""
                CREATE TABLE {table_name} (
                    office_company      TEXT,
                    house_company       TEXT,
                    offices_civil       TEXT,
                    house_civil         TEXT,
                    name_address        TEXT,
                    department_division TEXT,
                    note                TEXT
                )
            """)
            
            # إدراج البيانات مع التنظيف
            count = 0
            for row in rows[1:]:
                if any(row):  # تأكد من أن الصف ليس فارغاً
                    # تحويل جميع القيم إلى نصوص
                    row_data = [str(cell) if cell is not None else "" for cell in row]
                    
                    # تخطي العمود الأول (number) واستخدام الأعمدة من 2 إلى 8
                    row_data = row_data[1:8]  # تخطي العمود الأول
                    
                    # إذا كان عدد الأعمدة أقل من 7، أضف أعمدة فارغة
                    while len(row_data) < 7:
                        row_data.append("")
                    
                    # اقتصاص على 7 أعمدة فقط
                    row_data = row_data[:7]
                    
                    cursor.execute(f"""
                        INSERT INTO {table_name} 
                        (office_company, house_company, offices_civil, house_civil, name_address, department_division, note) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, row_data)
                    count += 1
            
            conn.commit()
            print(f"   ✅ تم إنشاء جدول: {table_name} ({count} سجل بعد التنظيف)")
        
        conn.close()
        print(f"✅ تم إنشاء قاعدة البيانات: {db_name}")
        
    except Exception as e:
        print(f"❌ خطأ في معالجة {xlsx_file_path}: {e}")

def convert_all_xlsx_to_sqlite(directory="."):
    """تحويل جميع ملفات XLSX في المجلد إلى قواعد بيانات SQLite"""
    xlsx_files = list(Path(directory).glob("*.xlsx"))
    
    if not xlsx_files:
        print(f"⚠️ لم يتم العثور على ملفات XLSX في المجلد: {directory}")
        return
    
    print(f"🔍 تم العثور على {len(xlsx_files)} ملف XLSX")
    
    for xlsx_file in xlsx_files:
        # استخدام اسم قاعدة البيانات بالإنجليزية
        db_name = "company_phones.db"
        db_path = xlsx_file.parent / db_name
        
        create_sqlite_from_xlsx(str(xlsx_file), str(db_path))

# --- تشغيل السكريبت ---
if __name__ == "__main__":
    # تحويل جميع ملفات XLSX في المجلد الحالي
    convert_all_xlsx_to_sqlite(".")