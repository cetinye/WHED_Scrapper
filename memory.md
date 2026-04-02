# WHED-Scrapping Memory

Bu dosya, baska bir bilgisayardaki yeni bir Codex oturumunun projeyi hizlica anlayip kaldigi yerden devam edebilmesi icin hazirlandi.

Onemli sinir:
- Bu ozet sadece mevcut kod tabani ve bu thread icindeki konusmalardan cikartildi.
- Ayri chatlerdeki gecmis konusmalara dogrudan erisimim yoktu. Bu yuzden burada yazanlar "repo + bu konusma" hafizasidir.

## Projenin amaci

Bu repo temel olarak `whed.net` verisini cekip yapilandirir, sonra da onu hem analiz dostu Excel ciktilara hem de veritabani benzeri pivot tablolara donusturur.

Ana is akisi kabaca soyledir:
1. `whed_scraper.py` ile WHED sayfalari TXT olarak cekilir.
2. `whed_excel_export.py` ile TXT dosyalari `whed_data.xlsx` ve turev tablolara donusturulur.
3. `whed_enrich.py` ve diger enrichment scriptleri ile admission, living cost, ranking, student-friendliness gibi kolonlar eklenir.
4. `txt_to_excel.py` icindeki DB merge modlari ile `References/TercihAnalizi Database Tables/` altindaki hedef DB workbook'leri guncellenir.

## Bu projede en onemli dosyalar

### Cekirdek dosyalar

- `txt_to_excel.py`
  Bu thread boyunca en cok duzenlenen dosya. Artik hem klasik export yapiyor hem de DB-style merge komutlarini calistiriyor.

- `whed_excel_export.py`
  TXT -> Excel donusumunu yapiyor. `Institutions`, `programs`, `university_programs` gibi temel export mantigi burada.

- `whed_scraper.py`
  WHED kurum sayfalarini topluyor.

- `whed_enrich.py`
  `whed_data.xlsx` uzerindeki admission/cost vb. zenginlestirmeleri yapiyor.

- `isced_f.py`
  Bachelor program adlarindan ISCED-F siniflandirmasi cikariyor. `References/Codes/` altindaki JSON referanslari kullaniyor.

### Enrichment scriptleri

- `add_student_costs.py`
  Yasam maliyeti kolonlarini doldurur.

- `add_student_friendliness.py`
  sehir/yasam/ulasim/cevre vb. skorlarla student-friendliness kolonlarini doldurur.

- `add_extended_metrics.py`
  ranking, research, economic ve bazi dis metrikleri ekler.

- `add_admission_outcomes.py`
  Ozellikle ABD icin College Scorecard / NCES tarafli admission ve graduation metriklerini ekler.

## Hedef DB workbook'leri

Bu konusmada calisilan ana DB dosyalari bunlardir:

- `References/TercihAnalizi Database Tables/universities.xlsx`
- `References/TercihAnalizi Database Tables/programs.xlsx`
- `References/TercihAnalizi Database Tables/university_programs.xlsx`
- `References/TercihAnalizi Database Tables/university_placement_conditions.xlsx`
- `References/TercihAnalizi Database Tables/cities.xlsx`

Onemli not:
- Kokte daha once `universities.xlsx`, `programs.xlsx`, `university_programs.xlsx` isimli export dosyalari da vardi.
- DB merge isleri icin esas alinan dosyalar `References/TercihAnalizi Database Tables/` altindakilerdir.

## `txt_to_excel.py` icinde eklenen DB merge modlari

Su merge modlari vardir:

- `--merge-db-universities`
- `--merge-db-programs`
- `--merge-db-university-programs`
- `--merge-db-university-placement-conditions`

Bunlar dogrudan `References/TercihAnalizi Database Tables/` altindaki workbook'leri guncellemek icin kullaniliyor.

## Bu thread icinde yapilan isler

### 1. Universities merge

Amac:
- `whed_data.xlsx` icindeki universiteleri mevcut DB `universities.xlsx` ile birlestirmek
- `id` degerlerini devam ettirmek
- `country_id` ve `city_id` degerlerini referans dosyalardan eslemek

Kurallar:
- `country_id` -> `countries.xlsx`
- `city_id` -> `cities.xlsx` + `districts.xlsx`
- `code` her `country_id` icinde `1`'den baslayacak sekilde yeniden numaralanir
- `name` universite adi
- `type`: `Public -> state`, `Private -> foundation`

Su anki sonuc snapshot:
- `universities.xlsx`: `4698` satir

Ek notlar:
- sehir eslesemeyen kayitlar icin bir ara `universities_unmatched_locations.xlsx` uretildi
- bu unmatched sehirler daha sonra `cities.xlsx` icine eklenerek cozuldu

### 2. Cities guncellemesi

Yapilanlar:
- `universities_unmatched_locations.xlsx` icindeki eksik sehirler `cities.xlsx` icine eklendi
- yeni sehirler icin `latitude` / `longitude` bulundu ve yazildi

Onemli karar:
- `fips_code` ve `iso2` alanlari once dolduruldu ama bunlar resmi olarak teyit edilmemisti
- sonra kullanici istegiyle emin olunmayan `fips_code` / `iso2` degerleri kaldirildi
- global sehirler icin FCC FIPS TXT kaynagi uygun olmadigi goruldu; sadece ABD county/state listesi veriyor

Su anki onemli durum:
- yeni eklenen sehirlerde dogrulanmamis `fips_code` / `iso2` tutulmuyor
- `latitude` / `longitude` dolduruldu

Su anki sonuc snapshot:
- `cities.xlsx`: `4854` satir

### 3. Programs merge

Amac:
- `whed_data.xlsx` icindeki bachelor programlari DB `programs.xlsx` icine eklemek
- unique olmalarina dikkat etmek

Ana kaynaklar:
- program adi -> `whed_data.xlsx`
- `isced_code` -> `isced_codes.xlsx`
- `country_id` -> `universities.xlsx`
- `holland_match_id` -> once `holland_matches.xlsx` ile

Sonra su katmanlar eklendi:
1. exact Holland match
2. heuristic Holland match
3. Holland match bulunamiyorsa self-inference

Cok onemli karar:
- Holland tarafinda otomatik eslesmeyen ama yine de elle mantikli sekilde siniflandirilan programlarda `holland_match_id = "?"` yaziliyor
- buna ragmen `riasec_code`, `value_code`, `map_point` bos birakilmiyor; inference ile dolduruluyor

Programs merge'in ozellikleri:
- once eski `provider_name = whed` satirlarini temizliyor
- sonra WHED satirlarini deterministik bicimde yeniden kuruyor
- yani tekrar calistirilabilir

Snapshot:
- `programs.xlsx`: `4206` satir

Veri uyarisi:
- `holland_match_id = ?` olan satirlar resmi Holland match degildir, kendi inference katmanimizin sonucudur

### 4. University Programs merge

Amac:
- `programs.xlsx` ve `universities.xlsx` uzerinden, WHED bachelor offering'lerinden `university_programs.xlsx` pivot tablosu uretmek

Kurallar:
- mevcut non-WHED satirlar korunur
- eski WHED satirlari silinip yeniden uretilir
- `university_id` -> DB `universities.xlsx`
- `program_id` -> DB `programs.xlsx`
- `year` -> `4`
- `university_program_code` -> `?`
- `conditions` -> bos
- `details.source` -> `"WHED"`

Onemli not:
- WHED bolumunde duplicate olmamasi icin key mantigi `country_id + program_id + university_id + year`
- tum tabloda duplicate gorulebilir; bu, eski non-WHED veri yapisindan gelebilir
- WHED tarafindan uretilen bolum kendi icinde unique olacak sekilde kurulmustur

Snapshot:
- `university_programs.xlsx`: `111940` satir

### 5. University Placement Conditions merge

Amac:
- `whed_data.xlsx` icindeki admission requirement bilgisini DB `university_placement_conditions.xlsx` tablosuna aktarmak

Kullanilan kaynaklar:
- `whed_data.xlsx` / `Admission Requirement IDs` sheet
- `whed_data.xlsx` / `Institutions` sheet

Onemli karar:
- `Admission Requirement IDs` master tablo olarak alindi
- `Institutions` sheet ile gercekten kullanilan `country + condition_id` ciftleri dogrulandi
- usage count mismatch kontrolu yapildi

Cok onemli tasarim karari:
- Bazi ulkelerde admission requirement ID'leri ulke bazinda degil bolge/eyalet bazinda tanimli
- ornek: `United States of America - Texas`, `United States of America - California`, `Belgium - Flemish Community`, `Canada - Ontario`
- bunlar `countries.xlsx` eslesmesinde tek bir `country_id` altina dusuyor
- bu yuzden plain `code=1,2,3` kullanilsaydi collision olusacakti

Bu sebeple:
- tek varyantli ulkelerde `code` oldugu gibi birakildi: orn. `1`, `2`
- cok varyantli ulkelerde `code` scope'landi:
  - `texas:1`
  - `california:1`
  - `ontario:1`
  - `flemish-community:1`
  - `french-community:1`

Provider:
- WHED satirlarinda `provider = whed`

Snapshot:
- `university_placement_conditions.xlsx`: `2186` satir
- bunun `1999` satiri WHED kaynaklidir

Not:
- Bu tabloyu doldurduk ama `university_programs.conditions` alanina henuz otomatik linkleme yapilmadi.
- Yani placement condition metinleri tabloda var, ama program pivotuna baglama isi ayri bir asama olabilir.

## Bugun itibariyla satir snapshot'i

02 Nisan 2026 itibariyla:

- `universities.xlsx`: `4698`
- `programs.xlsx`: `4206`
- `university_programs.xlsx`: `111940`
- `university_placement_conditions.xlsx`: `2186`
- `cities.xlsx`: `4854`

## Yeniden uretim komutlari

Tipik siralama:

```bash
python txt_to_excel.py --merge-db-universities
python txt_to_excel.py --merge-db-programs
python txt_to_excel.py --merge-db-university-programs
python txt_to_excel.py --merge-db-university-placement-conditions
```

Not:
- Bu merge modlari default olarak DB workbook'lerini yerinde gunceller.
- Istersen ilgili `--db-...-output-file` argumanlariyla ayri dosyaya da yazdirabilirsin.

## Baska bir Codex oturumuna verilecek kisa briefing

Yeni bir Codex'e en kisa guvenli baslangic prompt'u yaklasik soyle olabilir:

> Once `memory.md` dosyasini oku. DB merge isleri `txt_to_excel.py` icinde. Hedef dosyalar `References/TercihAnalizi Database Tables/` altinda. WHED tarafinda universities, programs, university_programs ve university_placement_conditions merge modlari var. Once mevcut workbook'leri ve `txt_to_excel.py` icindeki merge fonksiyonlarini incele, sonra degisiklik yap.

## Dikkat edilmesi gerekenler

- `programs.xlsx` icinde `holland_match_id = ?` olan satirlar bilincli birakildi; bunlar self-inferred
- `cities.xlsx` icindeki yeni sehirlerde dogrulanmamis `fips_code` / `iso2` degerleri temizlendi
- global sehirler icin FCC FIPS TXT kaynagi uygun degil
- `university_placement_conditions.xlsx` icin code namespacing bilincli bir tasarim kararidir; kaldirilirsa collision riski dogar
- DB merge isleri `References/TercihAnalizi Database Tables/` altindaki dosyalar uzerinden yapilmali, kokteki export workbook'lerle karistirilmamali

## Muhtemel sonraki isler

- `university_programs.conditions` alanini `university_placement_conditions` kodlariyla iliskilendirmek
- `programs.xlsx` icindeki `?` Holland match kayitlarini daha ileri heuristic veya manuel review ile azaltmak
- placement condition'lari program seviyesine mi kurum seviyesine mi baglayacagimiza karar vermek
- yeni Codex oturumlarinda bu dosyayi guncel tutmak
