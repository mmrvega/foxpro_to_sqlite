import sqlite3
import sys

# ضمان ترميز UTF-8 للإخراج
if sys.stdout.encoding != 'utf-8':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# 1. الاتصال بقاعدة البيانات
db_path = r'C:\Users\mmrma\Documents\GitHub\nocportal\backend\NOCPortal.API\bin\Debug\net9.0\data\master.db'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Starting comprehensive data import...")

    employee_id = "222013"

    # حذف البيانات القديمة لتجنب تكرار المفاتيح
    print(f"Cleaning existing data for employee {employee_id}...")
    tables_to_clean = [
        "employees", "academicCertificate", "jobRank", 
        "lettersOfAppreciation", "tenure", "trainingCourse", 
        "annualPerformance", "research", "committee", "jobPosition"
    ]
    for table in tables_to_clean:
        cursor.execute(f"DELETE FROM {table} WHERE employeeId = ?", (employee_id,))

    # 2. إدراج بيانات الموظف (جدول employees)
    employee_query = """
    INSERT INTO employees (employeeId, badgeId, name, jobTitle, jobPosition, department, division, unit, authority, birthDate, religion, ethnic, motherName, graduationYear, sex, active, FailedLoginAttempts)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    employee_data = (
        employee_id, employee_id, "محمود جهاد عبدالله حسين الكركو", "ملاحظ فني", "بدون منصب", 
        "الانتاج", "باي حسن", "باي حسن داود /اب 6", "هيئةالعمليات", "1998-06-09", 
        "مسـلم", "تركمانية", "صون كول بهاءالدين احمد", "2018", "ذكر", 1, 0
    )
    cursor.execute(employee_query, employee_data)

    # 3. إدراج الشهادة الأكاديمية (جدول academicCertificate)
    cert_query = """
    INSERT INTO academicCertificate (employeeId, university, collage, educationLevel, specialization, year)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    cert_data = (employee_id, None, "معهدنفط/كركوك", "دبــلوم", "تشغيل وسيطرة", "2018")
    cursor.execute(cert_query, cert_data)

    # 4. إدراج الدرجات الوظيفية (جدول jobRank)
    ranks_query = """
    INSERT INTO jobRank (employeeId, title, adminNo, adminDate, startDate, note)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    ranks_data = [
        (employee_id, "معاون ملاحظ فني", "1108", "2021-09-27", "2019-12-26", "ترقية/ شركة نفط الشمال"),
        (employee_id, "ملاحظ فني", "1553", "2023-11-30", "2023-11-26", "ترقية /شركة نفط الشمال"),
        (employee_id, "فني", "2125", "2019-06-02", "2019-06-09", "الشركة/تعيين معاهد النفط")
    ]
    cursor.executemany(ranks_query, ranks_data)

    # 5. إدراج كتب الشكر (جدول lettersOfAppreciation)
    letters_query = """
    INSERT INTO lettersOfAppreciation (employeeId, title, adminNo, adminDate, cause, issuingAuthority)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    letters_data = [
        (employee_id, "شـكـر وتقديـر", "4465", "2020-03-29", "الحد من انتشار فايروس كورونا", "السيد محافظ كركوك"),
        (employee_id, "شـكـر وتقديـر", "5309", "2020-02-24", "تثمين جهود", "رئاسة جامعة البصرة"),
        (employee_id, "شـكـر وتقديـر", "11606", "2020-04-26", "لتقديم الدعم للمؤسسات الصحية", "وزارة النفط"),
        (employee_id, "شـكـر وتقديـر", "9521", "2020-12-09", "لتعاون الشركة المستمر", "شركة الحفر العراقية"),
        (employee_id, "شـكـر وتقديـر", "1122", "2021-02-04", "للجهودالمبذولةاثناء الوباء", "دائرة صحة كركوك"),
        (employee_id, "شـكـر وتقديـر", "1121", "2021-02-04", "للجهودفي دعم القطاع الصحي", "صحةنينوى/م.الموصل"),
        (employee_id, "شـكـر وتقديـر", "32028", "2022-10-30", "للجهود في ادامة الانتاج", "وزير النفط"),
        (employee_id, "شـكـر وتقديـر", "15221", "2023-11-20", "لكونكم من الربع الاول في دورة", "ادارة الشركة"),
        (employee_id, "شـكـر وتقديـر", "8737", "2024-06-23", "انتخاب مجالس محافظات/قدم6اشهر", "رئيس مجلس الوزراء"),
        (employee_id, "شـكـر وتقديـر", "7984", "2024-06-02", "للجهود المبذولة/قدم(6) اشهر", "رئيس مجلس الوزراء")
    ]
    cursor.executemany(letters_query, letters_data)

    # 6. إدراج الخدمة (جدول tenure)
    tenure_query = "INSERT INTO tenure (employeeId, totall) VALUES (?, ?)"
    cursor.execute(tenure_query, (employee_id, "6- 5- 7"))

    # 7. إدراج الدورات التدريبية (جدول trainingCourse)
    courses_query = """
    INSERT INTO trainingCourse (employeeId, courseType, evaluation, title, startDate, endDate, location)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    courses_data = [
        (employee_id, "الكترونية", "ممتاز", "HSE", "2020-09-05", "2020-09-12", "وزارة النفط/التدريب"),
        (employee_id, "الكترونية", "متوسط", "طرق التحليل الالي - الفحص بجهازGC", "2021-06-08", "2021-06-17", "وزارة النفط/40ساعة"),
        (employee_id, "موقعية", "جيد", "HSE", "2021-02-07", "2021-02-11", "قاعة السلامة"),
        (employee_id, "موقعية", "متوسط", "مفهوم العقد الاداري", "2023-01-29", "2023-02-02", "معهد نفط كركوك"),
        (employee_id, "موقعية", "جيدجدا", "تكنلوجيا المضخات", "2023-02-19", "2023-02-23", "مقرهيئةالصحةوالسلامة"),
        (employee_id, "موقعية", "جيدجدا", "HSE", "2023-06-18", "2023-06-22", "قسم الهندسةالمدنية"),
        (employee_id, "مركزية", "ممتاز", "نظام ملاحةومواقع عبرقمر صناعي gnss-", "2023-10-22", "2023-11-02", "معهدالتدريب النفطي"),
        (employee_id, "موقعية", "جيدجدا", "HSE", "2024-04-14", "2024-04-18", "قسم السلامة الصناعية"),
        (employee_id, "موقعية", "جيدجدا", "WINDOWS_10", "2024-05-12", "2024-05-16", "قاعة الدورات IT"),
        (employee_id, "مركزية", "جيد", "تكنلوجيا المضخات", "2024-10-20", "2024-10-24", "معهدتدريب نفط كركوك"),
        (employee_id, "مركزية", "جيد", "الأكل وطرق الحد مــنه", "2025-02-23", "2025-02-27", "معهدتدريب نفطي/كركوك")
    ]
    cursor.executemany(courses_query, courses_data)

    # 8. إدراج التقييمات السنوية (جدول annualPerformance)
    performance_query = "INSERT INTO annualPerformance (employeeId, year, rating, redeg) VALUES (?, ?, ?, ?)"
    performance_data = [
        (employee_id, "2020-05-01", "جيد", "75"),
        (employee_id, "2022-11-01", "جيدجدا", "83"),
        (employee_id, "2021-12-01", "جيدجدا", "84"),
        (employee_id, "2023-11-01", "جيدجدا", "85"),
        (employee_id, "2024-11-01", "جيدجدا", "85")
    ]
    cursor.executemany(performance_query, performance_data)

    # 5. حفظ التغييرات
    conn.commit()
    print("All data imported successfully!")

except sqlite3.Error as e:
    print(f"Error during import: {e}")

finally:
    if conn:
        conn.close()