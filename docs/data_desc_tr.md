# Uçuş Verisi Dokümantasyonu

**Arka Plan:**
Sıkıştırılmış dosya içindeki veriler, TranStats veri kütüphanesindeki "On-Time" veritabanının Reporting Carrier On-Time Performance (1987-günümüz) veri tablosundan çıkarılmıştır.

## Seçilen Sütunlar Veri Sözlüğü

| Column Name | Açıklama |
| :--- | :--- |
| **Year** | Yıl |
| **Month** | Ay |
| **DayOfWeek** | Haftanın Günü |
| **FlightDate** | Uçuş Tarihi (yyyymmgg) |
| **Reporting_Airline** | Benzersiz Taşıyıcı Kodu. Aynı kod birden fazla taşıyıcı tarafından kullanıldığında, önceki kullanıcılar için sayısal bir ek kullanılır (örneğin PA, PA(1), PA(2)). Yıllar içindeki analizler için bu alanı kullanın. |
| **Origin** | Kalkış Havalimanı |
| **OriginCityName** | Kalkış Havalimanı, Şehir Adı |
| **Dest** | Varış Havalimanı |
| **DestCityName** | Varış Havalimanı, Şehir Adı |
| **CRSDepTime** | Planlanan (CRS) Kalkış Saati (yerel saat: ssdd) |
| **DepTime** | Gerçekleşen Kalkış Saati (yerel saat: ssdd) |
| **DepDelay** | Planlanan ve gerçekleşen kalkış saati arasındaki dakika cinsinden fark. Erken kalkışlar negatif sayılarla gösterilir. |
| **TaxiOut** | Kalkış havalimanında uçağın kapıdan (Gate) ayrılması ile havalanması (Wheels Off) arasında geçen süredir. |
| **TaxiIn** | Varış havalimanında uçağın iniş yapması (Wheels On) ile yolcu indirmek için kapıya yanaşması arasında geçen süredir. |
| **CRSArrTime** | Planlanan (CRS) Varış Saati (yerel saat: ssdd) |
| **ArrTime** | Gerçekleşen Varış Saati (yerel saat: ssdd) |
| **ArrDelay** | Planlanan ve gerçekleşen varış saati arasındaki dakika cinsinden fark. Erken varışlar negatif sayılarla gösterilir. |
| **Cancelled** | İptal Edilen Uçuş Göstergesi (1=Evet) |
| **CancellationCode** | İptal Nedenini Belirtir |
| **Diverted** | Yönlendirilen Uçuş Göstergesi (1=Evet) |
| **ActualElapsedTime** | Gerçekleşen Uçuş Süresi, Dakika Cinsinden |
| **AirTime** | Havada Kalma Süresi, Dakika Cinsinden |
| **Distance** | Havalimanları arasındaki mesafe (mil) |
| **CarrierDelay** | Taşıyıcı Kaynaklı Gecikme, Dakika Cinsinden |
| **WeatherDelay** | Hava Durumu Kaynaklı Gecikme, Dakika Cinsinden |
| **NASDelay** | Ulusal Hava Sistemi (NAS) Kaynaklı Gecikme, Dakika Cinsinden |
| **SecurityDelay** | Güvenlik Kaynaklı Gecikme, Dakika Cinsinden |
| **LateAircraftDelay** | Geç Gelen Uçak Kaynaklı Gecikme, Dakika Cinsinden |