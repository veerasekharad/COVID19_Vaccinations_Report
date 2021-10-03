spark-submit \
   --master yarn \
   --deploy-mode client \
   --packages com.crealytics:spark-excel_2.11:0.11.1 \
   spark/COVID_19_Vaccinations_Report.py \
   config/COVID_19_Vaccination.conf
   